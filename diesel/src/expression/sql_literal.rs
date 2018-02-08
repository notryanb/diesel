use std::marker::PhantomData;

use backend::Backend;
use expression::*;
use query_builder::*;
use query_dsl::RunQueryDsl;
use result::QueryResult;
use serialize::ToSql;
use sql_types::HasSqlType;

#[derive(Debug, Clone)]
/// Returned by the [`sql()`] function.
///
/// [`sql()`]: ../dsl/fn.sql.html
pub struct SqlLiteral<ST, T = ()> {
    sql: String,
    inner: T,
    _marker: PhantomData<ST>,
}

impl<ST, T> SqlLiteral<ST, T> {
    #[doc(hidden)]
    pub fn new(sql: String, inner: T) -> Self {
        SqlLiteral {
            sql: sql,
            inner: inner,
            _marker: PhantomData,
        }
    }

    /// Bind a value for use with this SQL query.
    ///
    /// # Safety
    ///
    /// This function should be used with care, as Diesel cannot validate that
    /// the value is of the right type nor can it validate that you have passed
    /// the correct number of parameters.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[macro_use] extern crate diesel;
    /// # include!("../doctest_setup.rs");
    /// #
    /// # table! {
    /// #    users {
    /// #        id -> Integer,
    /// #        name -> VarChar,
    /// #    }
    /// # }
    /// #
    /// # fn main() {
    /// #     use self::users::dsl::*;
    /// #     use diesel::dsl::sql;
    /// #     use diesel::sql_types::{Integer, Text};
    /// #     let connection = establish_connection();
    /// let seans_id = sql::<Integer>("SELECT id FROM users WHERE name = ")
    ///     .bind::<Text, _>("Sean")
    ///     .get_result(&connection);
    /// assert_eq!(Ok(1), seans_id);
    /// let tess_id = sql::<Integer>("SELECT id FROM users WHERE name = ")
    ///     .bind::<Text, _>("Tess")
    ///     .get_result(&connection);
    /// assert_eq!(Ok(2), tess_id);
    /// # }
    /// ```
    ///
    /// ### Multiple Bind Params
    ///
    /// ```rust
    /// # #[macro_use] extern crate diesel;
    /// # include!("../doctest_setup.rs");
    ///
    /// # table! {
    /// #    users {
    /// #        id -> Integer,
    /// #        name -> VarChar,
    /// #    }
    /// # }
    /// #
    /// # fn main() {
    /// #     use self::users::dsl::*;
    /// #     use diesel::dsl::sql;
    /// #     use diesel::sql_types::{Integer, Text};
    /// #     let connection = establish_connection();
    /// #     diesel::insert_into(users).values(name.eq("Ryan"))
    /// #           .execute(&connection).unwrap();
    /// let query = sql::<Text>("SELECT name FROM users WHERE id > ")
    ///     .bind::<Integer, _>(1)
    ///     .sql(" AND name <> ")
    ///     .bind::<Text, _>("Ryan")
    ///     .get_results(&connection);
    /// let expected = vec!["Tess".to_string()];
    /// assert_eq!(Ok(expected), query);
    /// # }
    /// ```
    pub fn bind<BindST, U>(self, bind_value: U) -> UncheckedBind<Self, U::Expression>
    where
        U: AsExpression<BindST>,
    {
        UncheckedBind::new(self, bind_value.as_expression())
    }

    pub fn sql(self, sql: &str) -> SqlLiteral<ST, Self> {
        SqlLiteral::new(sql.into(), self)
    }
}

impl<ST, T> Expression for SqlLiteral<ST, T> {
    type SqlType = ST;
}

impl<ST, T, DB> QueryFragment<DB> for SqlLiteral<ST, T>
where
    DB: Backend,
    T: QueryFragment<DB>,
{
    fn walk_ast(&self, mut out: AstPass<DB>) -> QueryResult<()> {
        out.unsafe_to_cache_prepared();
        self.inner.walk_ast(out.reborrow())?;
        out.push_sql(&self.sql);
        Ok(())
    }
}

impl<ST, T> QueryId for SqlLiteral<ST, T> {
    type QueryId = ();

    const HAS_STATIC_QUERY_ID: bool = false;
}

impl<ST, T> Query for SqlLiteral<ST, T> {
    type SqlType = ST;
}

impl<ST, Conn> RunQueryDsl<Conn> for SqlLiteral<ST> {}

impl<QS, ST> SelectableExpression<QS> for SqlLiteral<ST> {}

impl<QS, ST> AppearsOnTable<QS> for SqlLiteral<ST> {}

impl<ST> NonAggregate for SqlLiteral<ST> {}

/// Use literal SQL in the query builder
///
/// Available for when you truly cannot represent something using the expression
/// DSL. You will need to provide the SQL type of the expression, in addition to
/// the SQL.
///
/// This function is intended for use when you need a small bit of raw SQL in
/// your query. If you want to write the entire query using raw SQL, use
/// [`sql_query`](../fn.sql_query.html) instead.
///
/// # Safety
///
/// The compiler will be unable to verify the correctness of the annotated type.
/// If you give the wrong type, it'll either return an error when deserializing
/// the query result or produce unexpected values.
///
/// # Examples
///
/// ```rust
/// # #[macro_use] extern crate diesel;
/// # include!("../doctest_setup.rs");
/// # fn main() {
/// #     run_test().unwrap();
/// # }
/// #
/// # fn run_test() -> QueryResult<()> {
/// #     use schema::users::dsl::*;
/// use diesel::dsl::sql;
/// #     let connection = establish_connection();
/// let user = users.filter(sql("name = 'Sean'")).first(&connection)?;
/// let expected = (1, String::from("Sean"));
/// assert_eq!(expected, user);
/// #     Ok(())
/// # }
/// ```
pub fn sql<ST>(sql: &str) -> SqlLiteral<ST> {
    SqlLiteral::new(sql.into(), ())
}

#[derive(Debug, Clone, Copy)]
#[must_use = "Queries are only executed when calling `load`, `get_result`, or similar."]
pub struct UncheckedBind<Query, Value> {
    query: Query,
    value: Value,
}

impl<Query, Value> UncheckedBind<Query, Value>
where
    Query: Expression,
{
    pub fn new(query: Query, value: Value) -> Self {
        UncheckedBind {
            query,
            value,
        }
    }

    pub fn sql(self, sql: &str) -> SqlLiteral<Query::SqlType, Self> {
        SqlLiteral::new(sql.into(), self)
    }

    pub fn bind<ST2, U>(self, value: U) -> UncheckedBind<Self, U::Expression>
    where
        U: AsExpression<ST2>,
    {
        UncheckedBind::new(self, value.as_expression())
    }
}

// impl<Query, Value> AsExpression<Value> for UncheckedBind<Query, Value> {
//     type Expression = UncheckedBind<Query, Value>;
    
//     fn as_expression(self) -> Self::Expression {
//         UncheckedBind::new(self)
//     }
// }

impl<Query, Value> Expression for UncheckedBind<Query, Value> {
    type SqlType = Value;
}

impl<Query, Value> QueryId for UncheckedBind<Query, Value>
where
    Query: QueryId,
{
    type QueryId = UncheckedBind<Query::QueryId, ()>;

    const HAS_STATIC_QUERY_ID: bool = Query::HAS_STATIC_QUERY_ID;
}

impl<Query, Value, DB> QueryFragment<DB> for UncheckedBind<Query, Value>
where
    DB: Backend + HasSqlType<Value>,
    Query: QueryFragment<DB>,
    Value: ToSql<Value, DB>,
{
    fn walk_ast(&self, mut out: AstPass<DB>) -> QueryResult<()> {
        self.query.walk_ast(out.reborrow())?;
        out.push_bind_param(&self.value)?;
        Ok(())
    }
}

// impl<ST, T, DB> QueryFragment<DB> for SqlLiteral<ST, T>
// where
//     DB: Backend,
//     T: QueryFragment<DB>,
// {
//     fn walk_ast(&self, mut out: AstPass<DB>) -> QueryResult<()> {
//         out.unsafe_to_cache_prepared();
//         self.inner.walk_ast(out.reborrow())?;
//         out.push_sql(&self.sql);
//         Ok(())
//     }
// }

impl<Q, Value> Query for UncheckedBind<Q, Value>
where
    Q: Query,
{
    type SqlType = Q::SqlType;
}

impl<Query, Value, Conn> RunQueryDsl<Conn> for UncheckedBind<Query, Value> {}
