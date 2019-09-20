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

extern crate parking_lot;
extern crate edgeware_runtime;
extern crate edgeware_primitives;

extern crate substrate_inherents as inherents;
extern crate substrate_primitives as primitives;
extern crate srml_aura as runtime_aura;
extern crate srml_support as runtime_support;
extern crate sr_primitives as runtime_primitives;
extern crate substrate_client as client;

extern crate exit_future;
extern crate tokio;
extern crate substrate_consensus_common as consensus;
extern crate substrate_consensus_aura as aura;
extern crate substrate_consensus_aura_primitives as aura_primitives;
extern crate substrate_finality_grandpa as grandpa;
extern crate substrate_transaction_pool as transaction_pool;
extern crate substrate_consensus_authorities as consensus_authorities;

#[macro_use]
extern crate error_chain;

#[macro_use]
extern crate futures;

#[macro_use]
extern crate log;

#[cfg(test)]
extern crate substrate_keyring;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{self, Duration, Instant};

use aura::SlotDuration;
use client::{BlockchainEvents, ChainHead, BlockBody};
use client::blockchain::HeaderBackend;
use client::block_builder::api::BlockBuilder as BlockBuilderApi;
use client::runtime_api::Core;
use extrinsic_store::Store as ExtrinsicStore;
use parking_lot::Mutex;
use polkadot_primitives::{
	Hash, Block, BlockId, BlockNumber, Header, SessionKey
};
use polkadot_primitives::parachain::{Id as ParaId, Chain, DutyRoster, BlockData, Extrinsic as ParachainExtrinsic, CandidateReceipt, CollatorSignature};
use polkadot_primitives::parachain::{AttestedCandidate, Statement as PrimitiveStatement};
use primitives::{Pair, ed25519::{self, Public as AuthorityId}};
use runtime_primitives::traits::ProvideRuntimeApi;
use tokio::runtime::TaskExecutor;
use tokio::timer::{Delay, Interval};
use transaction_pool::txpool::{Pool, ChainApi as PoolChainApi};

use futures::prelude::*;
use futures::future::{self, Either};
use inherents::InherentData;
use runtime_aura::timestamp::TimestampInherentData;
use consensus_authorities::AuthoritiesApi;

pub use self::error::{ErrorKind, Error};

mod evaluation;
mod error;


// block size limit.
const MAX_TRANSACTIONS_SIZE: usize = 4 * 1024 * 1024;

/// Polkadot proposer factory.
pub struct ProposerFactory<C, N, P, TxApi: PoolChainApi> {
	transaction_pool: Arc<Pool<TxApi>>,
	key: Arc<ed25519::Pair>,
	aura_slot_duration: SlotDuration,
}

impl<C, N, P, TxApi> ProposerFactory<C, N, P, TxApi> where
	<C::Collation as IntoFuture>::Future: Send + 'static,
	P: BlockchainEvents<Block> + ChainHead<Block> + BlockBody<Block>,
	P: ProvideRuntimeApi + HeaderBackend<Block> + Send + Sync + 'static,
	P::Api: Core<Block> + BlockBuilderApi<Block> + AuthoritiesApi<Block>,
	TxApi: PoolChainApi,
{
	/// Create a new proposer factory.
	pub fn new(
		client: Arc<P>,
		transaction_pool: Arc<Pool<TxApi>>,
		key: Arc<ed25519::Pair>,
		aura_slot_duration: SlotDuration,
	) -> Self {

		ProposerFactory {
			transaction_pool,
			key,
			aura_slot_duration,
		}
	}
}

impl<C, N, P, TxApi> consensus::Environment<Block> for ProposerFactory<C, N, P, TxApi> where
	C: Collators + Send + 'static,
	TxApi: PoolChainApi<Block=Block>,
	P: ProvideRuntimeApi + HeaderBackend<Block> + Send + Sync + 'static,
	P::Api: BlockBuilderApi<Block>,
	<C::Collation as IntoFuture>::Future: Send + 'static,
	N::TableRouter: Send + 'static,
{
	type Proposer = Proposer<P, TxApi>;
	type Error = Error;

	fn init(
		&self,
		parent_header: &Header,
		authorities: &[AuthorityId],
	) -> Result<Self::Proposer, Error> {
		let parent_hash = parent_header.hash();
		let parent_id = BlockId::hash(parent_hash);
		let sign_with = self.key.clone();

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

/// The Polkadot proposer logic.
pub struct Proposer<C: Send + Sync, TxApi: PoolChainApi> where
	C: ProvideRuntimeApi + HeaderBackend<Block>,
{
	client: Arc<C>,
	parent_hash: Hash,
	parent_id: BlockId,
	parent_number: BlockNumber,
	transaction_pool: Arc<Pool<TxApi>>,
	slot_duration: SlotDuration,
}

impl<C, TxApi> consensus::Proposer<Block> for Proposer<C, TxApi> where
	TxApi: PoolChainApi<Block=Block>,
	C: ProvideRuntimeApi + HeaderBackend<Block> + Send + Sync,
	C::Api: BlockBuilderApi<Block>,
{
	type Error = Error;
	type Create = Either<
		CreateProposal<C, TxApi>,
		future::FutureResult<Block, Error>,
	>;

	fn propose(&self, inherent_data: InherentData, max_duration: Duration) -> Self::Create {
		const ATTEMPT_PROPOSE_EVERY: Duration = Duration::from_millis(100);
		const SLOT_DURATION_DENOMINATOR: u64 = 3; // wait up to 1/3 of the slot for candidates.

		let now = Instant::now();

		let believed_timestamp = match inherent_data.timestamp_inherent_data() {
			Ok(timestamp) => timestamp,
			Err(e) => return Either::B(future::err(ErrorKind::InherentError(e).into())),
		};

		// set up delay until next allowed timestamp.
		let current_timestamp = current_timestamp();
		let delay_future = if current_timestamp >= believed_timestamp {
			None
		} else {
			Some(Delay::new(
				Instant::now() + Duration::from_secs(current_timestamp - believed_timestamp)
			))
		};

		let timing = ProposalTiming {
			minimum: delay_future,
			attempt_propose: Interval::new(now + ATTEMPT_PROPOSE_EVERY, ATTEMPT_PROPOSE_EVERY),
		};

		Either::A(CreateProposal {
			parent_hash: self.parent_hash.clone(),
			parent_number: self.parent_number.clone(),
			parent_id: self.parent_id.clone(),
			client: self.client.clone(),
			transaction_pool: self.transaction_pool.clone(),
			believed_minimum_timestamp: believed_timestamp,
			timing,
			inherent_data: Some(inherent_data),
			// leave some time for the proposal finalisation
			deadline: Instant::now() + max_duration - max_duration / 3,
		})
	}
}

fn current_timestamp() -> u64 {
	time::SystemTime::now().duration_since(time::UNIX_EPOCH)
		.expect("now always later than unix epoch; qed")
		.as_secs()
		.into()
}

struct ProposalTiming {
	minimum: Option<Delay>,
	attempt_propose: Interval,
	last_included: usize,
}

impl ProposalTiming {
	// whether it's time to attempt a proposal.
	// shouldn't be called outside of the context of a task.
	fn poll(&mut self, included: usize) -> Poll<(), ErrorKind> {
		// first drain from the interval so when the minimum delay is up
		// we don't have any notifications built up.
		//

		// wait until the minimum time has passed.
		if let Some(mut minimum) = self.minimum.take() {
			if let Async::NotReady = minimum.poll().map_err(ErrorKind::Timer)? {
				self.minimum = Some(minimum);
				return Ok(Async::NotReady);
			}
		}

		if included == self.last_included {
			return self.enough_candidates.poll().map_err(ErrorKind::Timer);
		}

		// the amount of includable candidates has changed. schedule a wakeup
		// if it's not sufficient anymore.
		match self.dynamic_inclusion.acceptable_in(Instant::now(), included) {
			Some(instant) => {
				self.last_included = included;
				self.enough_candidates.reset(instant);
				self.enough_candidates.poll().map_err(ErrorKind::Timer)
			}
			None => Ok(Async::Ready(())),
		}
	}
}

/// Future which resolves upon the creation of a proposal.
pub struct CreateProposal<C: Send + Sync, TxApi: PoolChainApi> {
	parent_hash: Hash,
	parent_number: BlockNumber,
	parent_id: BlockId,
	client: Arc<C>,
	transaction_pool: Arc<Pool<TxApi>>,
	timing: ProposalTiming, // @Todo...
	believed_minimum_timestamp: u64,
	inherent_data: Option<InherentData>,
	deadline: Instant,
}

impl<C, TxApi> CreateProposal<C, TxApi> where
	TxApi: PoolChainApi<Block=Block>,
	C: ProvideRuntimeApi + HeaderBackend<Block> + Send + Sync,
	C::Api: BlockBuilderApi<Block>,
{
	fn propose_with(&mut self, candidates: Vec<AttestedCandidate>) -> Result<Block, Error> {
		use client::block_builder::BlockBuilder;
		use runtime_primitives::traits::{Hash as HashT, BlakeTwo256};

		const MAX_TRANSACTIONS: usize = 40;

		let mut inherent_data = self.inherent_data
			.take()
			.expect("CreateProposal is not polled after finishing; qed");
		inherent_data.put_data(edgeware_runtime::PARACHAIN_INHERENT_IDENTIFIER, &candidates).map_err(ErrorKind::InherentError)?;

		let runtime_api = self.client.runtime_api();

		let mut block_builder = BlockBuilder::at_block(&self.parent_id, &*self.client)?;

		{
			let inherents = runtime_api.inherent_extrinsics(&self.parent_id, inherent_data)?;
			for inherent in inherents {
				block_builder.push(inherent)?;
			}

			let mut unqueue_invalid = Vec::new();
			let mut pending_size = 0;

			let ready_iter = self.transaction_pool.ready();
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
						pending_size += encoded_size;
					}
					Err(e) => {
						trace!(target: "transaction-pool", "Invalid transaction: {}", e);
						unqueue_invalid.push(ready.hash.clone());
					}
				}
			}

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

		Ok(new_block)
	}
}

impl<C, TxApi> Future for CreateProposal<C, TxApi> where
	TxApi: PoolChainApi<Block=Block>,
	C: ProvideRuntimeApi + HeaderBackend<Block> + Send + Sync,
	C::Api: BlockBuilderApi<Block>,
{
	type Item = Block;
	type Error = Error;

	fn poll(&mut self) -> Poll<Block, Error> {
		// 1. try to propose if we have enough includable candidates and other
		// delays have concluded.
		let included = self.table.includable_count();
		try_ready!(self.timing.poll(included));

		// 2. propose
		let proposed_candidates = self.table.proposed_set();

		self.propose_with(proposed_candidates).map(Async::Ready)
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
