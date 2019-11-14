// Copyright 2018-2019 Commonwealth Labs, Inc.
// This file is part of Edgeware.

// Edgeware is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// Edgeware is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with Edgeware.  If not, see <http://www.gnu.org/licenses/>

//! A set of constant values used in substrate runtime.

/// Money matters.
pub mod currency {
	use edgeware_primitives::Balance;

	pub const MILLICENTS: Balance = 10_000_000_000_000;
	pub const CENTS: Balance = 1_000 * MILLICENTS;    // assume this is worth about a cent.
	pub const DOLLARS: Balance = 100 * CENTS;
}

/// Time.
pub mod time {
	use edgeware_primitives::{Moment, BlockNumber};
	pub const MILLISECS_PER_BLOCK: Moment = 6000;
	pub const SLOT_DURATION: Moment = MILLISECS_PER_BLOCK;
	// These times are defined in block numbers
	pub const MINUTES: BlockNumber = 60_000 / (MILLISECS_PER_BLOCK as BlockNumber);
	pub const HOURS: BlockNumber = MINUTES * 60;
	pub const DAYS: BlockNumber = HOURS * 24;
}
