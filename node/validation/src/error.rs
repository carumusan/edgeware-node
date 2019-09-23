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

//! Errors that can occur during the consensus process.

use runtime_primitives::RuntimeString;

#[derive(Debug, derive_more::Display, derive_more::From)]
pub enum Error {
	/// Client error
	Client(client::error::Error),
	/// Consensus error
	Consensus(consensus::error::Error),
	#[display(fmt = "Invalid duty roster length: expected {}, got {}", expected, got)]
	InvalidDutyRosterLength {
		/// Expected roster length
		expected: usize,
		/// Actual roster length
		got: usize,
	},
	#[display(fmt = "Proposer destroyed before finishing proposing or evaluating")]
	PrematureDestruction,
	/// Unexpected error checking inherents
	#[display(fmt = "Unexpected error while checking inherents: {}", _0)]
	InherentError(RuntimeString),
	/// Timer failed
	#[display(fmt = "Timer failed: {}", _0)]
	Timer(std::io::Error),
	#[display(fmt = "Failed to compute deadline of now + {:?}", _0)]
	DeadlineComputeFailure(std::time::Duration),
	/// Unable to dispatch agreement future
	#[display(fmt = "Unable to dispatch agreement future: {:?}", _0)]
	Executor(futures::future::ExecuteErrorKind),
}

impl std::error::Error for Error {
	fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
		match self {
			Error::Client(ref err) => Some(err),
			Error::Consensus(ref err) => Some(err),
			_ => None,
		}
	}
}

// impl From<::bft::InputStreamConcluded> for Error {
// 	fn from(err: ::bft::InputStreamConcluded) -> Self {
// 		::bft::Error::from(err).into()
// 	}
// }
