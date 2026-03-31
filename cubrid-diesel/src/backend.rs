//! CUBRID backend definition for Diesel.
//!
//! Defines the [`Cubrid`] unit struct and implements the required Diesel
//! traits: [`Backend`], [`SqlDialect`], [`TypeMetadata`],
//! [`TrustedBackend`], and [`DieselReserveSpecialization`].

use diesel::backend::*;
use diesel::query_builder::bind_collector::RawBytesBindCollector;
use diesel::sql_types::TypeMetadata;

use crate::query_builder::CubridQueryBuilder;
use crate::value::CubridValue;

// ---------------------------------------------------------------------------
// Backend type
// ---------------------------------------------------------------------------

/// The Diesel backend type for CUBRID databases.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Default)]
pub struct Cubrid;

// ---------------------------------------------------------------------------
// CubridTypeMetadata
// ---------------------------------------------------------------------------

/// Type metadata used by the CUBRID backend.
///
/// Maps to CUBRID's wire-format type codes. Used by [`RawBytesBindCollector`]
/// to associate type information with serialized bind parameters.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum CubridTypeMetadata {
    /// 16-bit signed integer (SMALLINT). Also used for boolean.
    Short,
    /// 32-bit signed integer (INTEGER).
    Int,
    /// 64-bit signed integer (BIGINT).
    BigInt,
    /// 32-bit IEEE 754 float.
    Float,
    /// 64-bit IEEE 754 double.
    Double,
    /// Variable-length character string (VARCHAR / TEXT).
    String,
    /// Variable-length binary (VARBIT / BLOB).
    Binary,
    /// Calendar date.
    Date,
    /// Time of day.
    Time,
    /// Date and time without fractional seconds.
    Timestamp,
    /// Arbitrary precision numeric / decimal.
    Numeric,
}

// ---------------------------------------------------------------------------
// TypeMetadata
// ---------------------------------------------------------------------------

impl TypeMetadata for Cubrid {
    type TypeMetadata = CubridTypeMetadata;
    type MetadataLookup = ();
}

// ---------------------------------------------------------------------------
// SqlDialect
// ---------------------------------------------------------------------------

impl SqlDialect for Cubrid {
    type ReturningClause = sql_dialect::returning_clause::DoesNotSupportReturningClause;

    type OnConflictClause = sql_dialect::on_conflict_clause::DoesNotSupportOnConflictClause;

    type InsertWithDefaultKeyword = sql_dialect::default_keyword_for_insert::IsoSqlDefaultKeyword;

    type BatchInsertSupport =
        sql_dialect::batch_insert_support::PostgresLikeBatchInsertSupport;

    type ConcatClause = sql_dialect::concat_clause::ConcatWithPipesClause;

    type DefaultValueClauseForInsert =
        sql_dialect::default_value_clause::AnsiDefaultValueClause;

    type EmptyFromClauseSyntax =
        sql_dialect::from_clause_syntax::AnsiSqlFromClauseSyntax;

    type ExistsSyntax = sql_dialect::exists_syntax::AnsiSqlExistsSyntax;

    type ArrayComparison = sql_dialect::array_comparison::AnsiSqlArrayComparison;

    type SelectStatementSyntax =
        sql_dialect::select_statement_syntax::AnsiSqlSelectStatement;

    type AliasSyntax = sql_dialect::alias_syntax::AsAliasSyntax;

    type WindowFrameClauseGroupSupport =
        sql_dialect::window_frame_clause_group_support::NoGroupWindowFrameUnit;

    type WindowFrameExclusionSupport =
        sql_dialect::window_frame_exclusion_support::NoFrameFrameExclusionSupport;

    type AggregateFunctionExpressions =
        sql_dialect::aggregate_function_expressions::NoAggregateFunctionExpressions;

    type BuiltInWindowFunctionRequireOrder =
        sql_dialect::built_in_window_function_require_order::NoOrderRequired;
}

// ---------------------------------------------------------------------------
// Backend
// ---------------------------------------------------------------------------

impl Backend for Cubrid {
    type QueryBuilder = CubridQueryBuilder;
    type RawValue<'a> = CubridValue<'a>;
    type BindCollector<'a> = RawBytesBindCollector<Cubrid>;
}

// ---------------------------------------------------------------------------
// Marker traits
// ---------------------------------------------------------------------------

impl TrustedBackend for Cubrid {}
impl DieselReserveSpecialization for Cubrid {}
