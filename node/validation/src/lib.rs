// Copyright 2017 Parity Technologies (UK) Ltd.
// This file is part of Polkadot.

// Polkadot is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Polkadot is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Polkadot.  If not, see <http://www.gnu.org/licenses/>.

//! Propagation and agreement of candidates.
//!
//! Authorities are split into groups by parachain, and each authority might come
//! up its own candidate for their parachain. Within groups, authorities pass around
//! their candidates and produce statements of validity.
//!
//! Any candidate that receives majority approval by the authorities in a group
//! may be subject to inclusion, unless any authorities flag that candidate as invalid.
//!
//! Wrongly flagging as invalid should be strongly disincentivized, so that in the
//! equilibrium state it is not expected to happen. Likewise with the submission
//! of invalid blocks.
//!
//! Groups themselves may be compromised by malicious authorities.

extern crate edgeware_runtime;
extern crate edgeware_primitives;

extern crate substrate_inherents as inherents;
extern crate substrate_primitives as primitives;
extern crate srml_aura as runtime_aura;
extern crate srml_support as runtime_support;
extern crate sr_primitives as runtime_primitives;
extern crate substrate_client as client;

extern crate exit_future;
extern crate substrate_consensus_common as consensus;
extern crate substrate_consensus_aura as aura;
extern crate substrate_consensus_aura_primitives as aura_primitives;
extern crate substrate_finality_grandpa as grandpa;
extern crate substrate_transaction_pool as transaction_pool;

#[macro_use]
extern crate log;

use std::sync::Arc;
use std::pin::Pin;
use std::time::{self, Duration, Instant};
use codec::Encode;
use futures_timer::{Delay, Interval};
use futures03::{future::{self, Either, FutureExt}, task::Context, stream::StreamExt};
use client::{BlockBody, BlockchainEvents};
use client::blockchain::HeaderBackend;
use client::block_builder::api::BlockBuilder as BlockBuilderApi;
use edgeware_primitives::{
	Hash, BlockId, BlockNumber, Block
};
use primitives::{ed25519::{self, Public as AuthorityId}};
use runtime_primitives::traits::{Block as BlockT, ProvideRuntimeApi, BlakeTwo256, DigestFor};
use transaction_pool::txpool::{Pool, ChainApi as PoolChainApi};
use keystore::KeyStorePtr;

use inherents::InherentData;
use runtime_aura::timestamp::TimestampInherentData;

pub use self::error::{Error};

mod evaluation;
mod error;


// block size limit.
const MAX_TRANSACTIONS_SIZE: usize = 4 * 1024 * 1024;

/// Edgeware proposer factory.
pub struct ProposerFactory<P, TxApi: PoolChainApi> {
    client: Arc<P>,
	transaction_pool: Arc<Pool<TxApi>>,
	keystore: KeyStorePtr,
	aura_slot_duration: u64,
}

impl<P, TxApi> ProposerFactory<P, TxApi> where
	P: BlockchainEvents<Block> + BlockBody<Block>,
	P: ProvideRuntimeApi + HeaderBackend<Block> + Send + Sync + 'static,
	P::Api: BlockBuilderApi<Block>,
	TxApi: PoolChainApi,
{
	/// Create a new proposer factory.
	pub fn new(
		client: Arc<P>,
		transaction_pool: Arc<Pool<TxApi>>,
		keystore: KeyStorePtr,
		aura_slot_duration: u64,
	) -> Self {

		ProposerFactory {
            client: client.clone(),
			transaction_pool,
			keystore,
			aura_slot_duration,
		}
	}
}

impl<P, TxApi> consensus::Environment<Block> for 
ProposerFactory<P, TxApi> 
where
    Block: BlockT,
	P: ProvideRuntimeApi + HeaderBackend<Block> + Send + Sync + 'static,
	P::Api: BlockBuilderApi<Block>,
    Block: BlockT,
    TxApi: PoolChainApi<Block=Block>,
{
	type Proposer = Proposer<P, TxApi>;
	type Error = Error;

	fn init(
		&mut self,
        parent_header: &<Block as BlockT>::Header,
	) -> Result<Self::Proposer, Error> {
		let parent_hash = parent_header.hash();
		let parent_id = BlockId::hash(parent_hash);

        info!("Starting consensus session on top of parent {:?}", parent_hash);

		Ok(Proposer {
			client: self.client.clone(),
			parent_hash,
			parent_id,
			parent_number: parent_header.number,
			transaction_pool: self.transaction_pool.clone(),
			slot_duration: self.aura_slot_duration,
		})
	}
}

/// The Edgeware proposer logic.
pub struct Proposer<C: Send + Sync, TxApi: PoolChainApi> where
	C: ProvideRuntimeApi + HeaderBackend<Block>,
{
	client: Arc<C>,
	parent_hash: <Block as BlockT>::Hash,
	parent_id: BlockId,
	parent_number: BlockNumber,
	transaction_pool: Arc<Pool<TxApi>>,
	slot_duration: u64,
}

impl<C, TxApi> consensus::Proposer<Block> for Proposer<C, TxApi> where
	TxApi: PoolChainApi<Block=Block>,
	C: ProvideRuntimeApi + HeaderBackend<Block> + Send + Sync,
	C::Api: BlockBuilderApi<Block>,
{
	type Error = Error;
	type Create = Either<CreateProposal<C, TxApi>, future::Ready<Result<Block, Error>>>;

	fn propose(&mut self, 
        inherent_data: InherentData, 		
        inherent_digests: DigestFor<Block>,
        max_duration: Duration,
    ) -> Self::Create {
		const ATTEMPT_PROPOSE_EVERY: Duration = Duration::from_millis(100);
        debug!("Attempting propose every: {:?}", ATTEMPT_PROPOSE_EVERY);
        debug!("Longest to propose for: {:?}", max_duration);

        let now = Instant::now();

		let believed_timestamp = match inherent_data.timestamp_inherent_data() {
			Ok(timestamp) => timestamp,
            Err(e) => return Either::Right(future::err(Error::InherentError(e))),
		};

		// set up delay until next allowed timestamp.
		let current_timestamp = current_timestamp();
		let delay_future = if current_timestamp >= believed_timestamp {
			None
		} else {
            debug!("current_timestep: {:?}", current_timestamp);
            debug!("believed_timestep: {:?}", believed_timestamp);
			Some(Delay::new(Duration::from_millis (current_timestamp - believed_timestamp)))
		};

		let timing = ProposalTiming {
			minimum: delay_future,
			attempt_propose: Interval::new(ATTEMPT_PROPOSE_EVERY),
		};

        let deadline_diff = max_duration - max_duration / 3;
		let deadline = match Instant::now().checked_add(deadline_diff) {
			None => return Either::Right(
				future::err(Error::DeadlineComputeFailure(deadline_diff)),
			),
			Some(d) => d,
		};
        
		Either::Left(CreateProposal {
			parent_hash: self.parent_hash.clone(),
			parent_number: self.parent_number.clone(),
			parent_id: self.parent_id.clone(),
			client: self.client.clone(),
			transaction_pool: self.transaction_pool.clone(),
            believed_minimum_timestamp: believed_timestamp,
            timing,
			inherent_data: Some(inherent_data),
            inherent_digests,
			// leave some time for the proposal finalisation
			deadline,
		})
	}
}

fn current_timestamp() -> u64 {
	time::SystemTime::now().duration_since(time::UNIX_EPOCH)
		.expect("now always later than unix epoch; qed")
		.as_millis() as u64
}

struct ProposalTiming {
	minimum: Option<Delay>,
	attempt_propose: Interval,
}

impl ProposalTiming {
	// whether it's time to attempt a proposal.
	// shouldn't be called outside of the context of a task.
	fn poll(&mut self, cx: &mut Context) -> futures03::Poll<Result<(), Error>> {
		// first drain from the interval so when the minimum delay is up
		// we don't have any notifications built up.
		//
        while let futures03::Poll::Ready(x) = self.attempt_propose.poll_next_unpin(cx) {
			x.expect("timer still alive; intervals never end; qed");
		}

        debug!("checking proposal timing");
		// wait until the minimum time has passed.
		if let Some(mut minimum) = self.minimum.take() {
			if let futures03::Poll::Pending = minimum.poll_unpin(cx) {
				self.minimum = Some(minimum);
				return futures03::Poll::Pending;
			}
		}
        return futures03::Poll::Ready(Ok(()));
	}
}

/// Future which resolves upon the creation of a proposal.
pub struct CreateProposal<C: Send + Sync, TxApi: PoolChainApi> {
	parent_hash: Hash,
	parent_number: BlockNumber,
	parent_id: BlockId,
	client: Arc<C>,
	transaction_pool: Arc<Pool<TxApi>>,
	timing: ProposalTiming,
	believed_minimum_timestamp: u64,
	inherent_data: Option<InherentData>,
    inherent_digests: DigestFor<Block>,
	deadline: Instant,
}

impl<C, TxApi> CreateProposal<C, TxApi> where
	TxApi: PoolChainApi<Block=Block>,
	C: ProvideRuntimeApi + HeaderBackend<Block> + Send + Sync,
	C::Api: BlockBuilderApi<Block>,
{
	fn propose_with(&mut self) -> Result<Block, Error> {
		use client::block_builder::BlockBuilder;
        use runtime_primitives::traits::{Hash as HashT};

		const MAX_TRANSACTIONS: usize = 40;

		let mut inherent_data = self.inherent_data
			.take()
			.expect("CreateProposal is not polled after finishing; qed");

		let runtime_api = self.client.runtime_api();

		let mut block_builder = BlockBuilder::at_block(
            &self.parent_id, 
            &*self.client,
            false,
            self.inherent_digests.clone(),
        )?;
        debug!("created block builder.");

		{
			let inherents = runtime_api.inherent_extrinsics(&self.parent_id, inherent_data)?;
			for inherent in inherents {
				block_builder.push(inherent)?;
			}

			let mut unqueue_invalid = Vec::new();
			let mut pending_size = 0;

			let ready_iter = self.transaction_pool.ready();
            debug!("Attempting to push transactions from the pool.");
			for ready in ready_iter.take(MAX_TRANSACTIONS) {
				let encoded_size = ready.data.encode().len();
				if pending_size + encoded_size >= MAX_TRANSACTIONS_SIZE {
					break
				}
				if Instant::now() > self.deadline {
					debug!("Consensus deadline reached when pushing block transactions, proceeding with proposing.");
					break;
				}

				match block_builder.push(ready.data.clone()) {
					Ok(()) => {
                        debug!("added tx.");
						pending_size += encoded_size;
                        debug!("pending size now {:?}", pending_size);
					}
					Err(e) => {
						trace!(target: "transaction-pool", "Invalid transaction: {}", e);
						unqueue_invalid.push(ready.hash.clone());
					}
				}
			}

            debug!("remove invalid");
			self.transaction_pool.remove_invalid(&unqueue_invalid);
		}

		let new_block = block_builder.bake()?;

		info!("Prepared block for proposing at {} [hash: {:?}; parent_hash: {}; extrinsics: [{}]]",
			new_block.header.number,
			Hash::from(new_block.header.hash()),
			new_block.header.parent_hash,
			new_block.extrinsics.iter()
				.map(|xt| format!("{}", BlakeTwo256::hash_of(xt)))
				.collect::<Vec<_>>()
				.join(", ")
		);

        assert!(evaluation::evaluate_initial(
			&new_block,
			self.believed_minimum_timestamp,
			&self.parent_hash,
			self.parent_number,
		).is_ok());

		Ok(new_block)
	}
}

impl<C, TxApi> futures03::Future for CreateProposal<C, TxApi> where
	TxApi: PoolChainApi<Block=Block>,
	C: ProvideRuntimeApi + HeaderBackend<Block> + Send + Sync,
	C::Api: BlockBuilderApi<Block>,
{
	type Output = Result<Block, Error>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> futures03::Poll<Self::Output> {
		// 1. try to propose if we have enough includable candidates and other
		// delays have concluded.
        debug!("Polling now {}", cx);
		futures03::ready!(self.timing.poll(cx))?;

		futures03::Poll::Ready(self.propose_with())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use std::cell::RefCell;
	use consensus_common::{Environment, Proposer};
	use test_client::{self, runtime::{Extrinsic, Transfer}, AccountKeyring};

	fn extrinsic(nonce: u64) -> Extrinsic {
		Transfer {
			amount: Default::default(),
			nonce,
			from: AccountKeyring::Alice.into(),
			to: Default::default(),
		}.into_signed_tx()
	}

    // @TODO Create tests
	#[test]
	fn should_cease_building_block_when_deadline_is_reached() {
		// given
		let client = Arc::new(test_client::new());
		let chain_api = transaction_pool::ChainApi::new(client.clone());
		let txpool = Arc::new(TransactionPool::new(Default::default(), chain_api));

		txpool.submit_at(&BlockId::number(0), vec![extrinsic(0), extrinsic(1)], false).unwrap();

		let mut proposer_factory = ProposerFactory {
			client: client.clone(),
			transaction_pool: txpool.clone(),
		};

		let mut proposer = proposer_factory.init(
			&client.header(&BlockId::number(0)).unwrap().unwrap(),
		).unwrap();

		// when
		let cell = RefCell::new(time::Instant::now());
		proposer.now = Box::new(move || {
			let new = *cell.borrow() + time::Duration::from_secs(2);
			cell.replace(new)
		});
		let deadline = time::Duration::from_secs(3);
		let block = futures::executor::block_on(proposer.propose(Default::default(), Default::default(), deadline))
			.unwrap();

		// then
		// block should have some extrinsics although we have some more in the pool.
		assert_eq!(block.extrinsics().len(), 1);
		assert_eq!(txpool.ready().count(), 2);
	}
}
