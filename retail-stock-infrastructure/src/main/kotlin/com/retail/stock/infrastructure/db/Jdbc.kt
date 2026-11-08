package com.retail.stock.infrastructure.db

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Types
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.*

internal inline fun <T> Connection.prepared(sql: String, block: (PreparedStatement) -> T): T =
    prepareStatement(sql).use(block)

internal inline fun <T> PreparedStatement.query(mapper: (ResultSet) -> T): List<T> =
    executeQuery().use { rs ->
        val rows = ArrayList<T>()
        while (rs.next()) rows += mapper(rs)
        rows
    }

internal fun PreparedStatement.setInstant(index: Int, value: Instant) =
    setObject(index, OffsetDateTime.ofInstant(value, ZoneOffset.UTC))

internal fun PreparedStatement.setNullableInstant(index: Int, value: Instant?) =
    if (value == null) setNull(index, Types.TIMESTAMP_WITH_TIMEZONE) else setInstant(index, value)

internal fun PreparedStatement.setNullableString(index: Int, value: String?) =
    if (value == null) setNull(index, Types.VARCHAR) else setString(index, value)

internal fun PreparedStatement.setNullableUuid(index: Int, value: UUID?) =
    if (value == null) setNull(index, Types.OTHER) else setObject(index, value)

internal fun PreparedStatement.setNullableInt(index: Int, value: Int?) =
    if (value == null) setNull(index, Types.INTEGER) else setInt(index, value)

internal fun PreparedStatement.setNullableLong(index: Int, value: Long?) =
    if (value == null) setNull(index, Types.BIGINT) else setLong(index, value)

internal fun ResultSet.instant(column: String): Instant =
    getObject(column, OffsetDateTime::class.java).toInstant()

internal fun ResultSet.uuid(column: String): UUID = getObject(column, UUID::class.java)

internal fun ResultSet.nullableUuid(column: String): UUID? = getObject(column, UUID::class.java)

internal fun ResultSet.nullableInt(column: String): Int? = getInt(column).takeUnless { wasNull() }

internal fun ResultSet.nullableLong(column: String): Long? = getLong(column).takeUnless { wasNull() }

internal fun Connection.textArray(values: Collection<String>): java.sql.Array =
    createArrayOf("text", values.toTypedArray())

internal fun Connection.bigintArray(values: Collection<Long>): java.sql.Array =
    createArrayOf("bigint", values.toTypedArray())
