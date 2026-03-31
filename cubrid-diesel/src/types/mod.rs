//! Diesel type mappings for CUBRID.
//!
//! Implements [`HasSqlType`] for all required Diesel SQL types and provides
//! [`FromSql`] / [`ToSql`] implementations bridging between Diesel's type
//! system and CUBRID's wire format.

mod numeric;
mod string;
mod binary;
mod bool;
mod temporal;

use diesel::sql_types;
use diesel::sql_types::HasSqlType;

use crate::backend::{Cubrid, CubridTypeMetadata};

// ---------------------------------------------------------------------------
// HasSqlType implementations
// ---------------------------------------------------------------------------

impl HasSqlType<sql_types::SmallInt> for Cubrid {
    fn metadata(_lookup: &mut ()) -> CubridTypeMetadata {
        CubridTypeMetadata::Short
    }
}

impl HasSqlType<sql_types::Integer> for Cubrid {
    fn metadata(_lookup: &mut ()) -> CubridTypeMetadata {
        CubridTypeMetadata::Int
    }
}

impl HasSqlType<sql_types::BigInt> for Cubrid {
    fn metadata(_lookup: &mut ()) -> CubridTypeMetadata {
        CubridTypeMetadata::BigInt
    }
}

impl HasSqlType<sql_types::Float> for Cubrid {
    fn metadata(_lookup: &mut ()) -> CubridTypeMetadata {
        CubridTypeMetadata::Float
    }
}

impl HasSqlType<sql_types::Double> for Cubrid {
    fn metadata(_lookup: &mut ()) -> CubridTypeMetadata {
        CubridTypeMetadata::Double
    }
}

impl HasSqlType<sql_types::Text> for Cubrid {
    fn metadata(_lookup: &mut ()) -> CubridTypeMetadata {
        CubridTypeMetadata::String
    }
}

impl HasSqlType<sql_types::Binary> for Cubrid {
    fn metadata(_lookup: &mut ()) -> CubridTypeMetadata {
        CubridTypeMetadata::Binary
    }
}

impl HasSqlType<sql_types::Date> for Cubrid {
    fn metadata(_lookup: &mut ()) -> CubridTypeMetadata {
        CubridTypeMetadata::Date
    }
}

impl HasSqlType<sql_types::Time> for Cubrid {
    fn metadata(_lookup: &mut ()) -> CubridTypeMetadata {
        CubridTypeMetadata::Time
    }
}

impl HasSqlType<sql_types::Timestamp> for Cubrid {
    fn metadata(_lookup: &mut ()) -> CubridTypeMetadata {
        CubridTypeMetadata::Timestamp
    }
}

impl HasSqlType<sql_types::Bool> for Cubrid {
    fn metadata(_lookup: &mut ()) -> CubridTypeMetadata {
        CubridTypeMetadata::Short
    }
}

impl HasSqlType<sql_types::Numeric> for Cubrid {
    fn metadata(_lookup: &mut ()) -> CubridTypeMetadata {
        CubridTypeMetadata::Numeric
    }
}

impl HasSqlType<sql_types::TinyInt> for Cubrid {
    fn metadata(_lookup: &mut ()) -> CubridTypeMetadata {
        CubridTypeMetadata::Short
    }
}
