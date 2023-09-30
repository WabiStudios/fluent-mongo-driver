/* -----------------------------------------------------------
 * :: :  G  H  O  S  T  :                                   ::
 * -----------------------------------------------------------
 * @wabistudios :: cosmos :: realms
 *
 * CREDITS.
 *
 * T.Furby              @furby-tm       <devs@wabi.foundation>
 * D.Kirkpatrick  @dkirkpatrick99  <d.kirkpatrick99@gmail.com>
 *
 *         Copyright (C) 2023 Wabi Animation Studios, Ltd. Co.
 *                                        All Rights Reserved.
 * -----------------------------------------------------------
 *  . x x x . o o o . x x x . : : : .    o  x  o    . : : : .
 * ----------------------------------------------------------- */

import FluentKit
import MongoCore
import MongoKitten

extension FluentMongoDatabase
{
  func aggregate(
    query: DatabaseQuery,
    aggregate: DatabaseQuery.Aggregate,
    onOutput: @Sendable @escaping (DatabaseOutput) -> Void
  ) async throws
  {
    guard case let .field(field, method) = aggregate
    else { throw FluentMongoError.unsupportedCustomAggregate }

    switch method
    {
      case .count where query.joins.isEmpty:
        return try await count(query: query, onOutput: onOutput)
      case .count:
        return try await joinCount(query: query, onOutput: onOutput)
      case .sum:
        return try await group(
          query: query,
          mongoOperator: "$sum",
          field: field,
          onOutput: onOutput
        )
      case .average:
        return try await group(
          query: query,
          mongoOperator: "$avg",
          field: field,
          onOutput: onOutput
        )
      case .maximum:
        return try await group(
          query: query,
          mongoOperator: "$max",
          field: field,
          onOutput: onOutput
        )
      case .minimum:
        return try await group(
          query: query,
          mongoOperator: "$min",
          field: field,
          onOutput: onOutput
        )
      case .custom:
        throw FluentMongoError.unsupportedCustomAggregate
    }
  }

  private func count(
    query: DatabaseQuery,
    onOutput: @Sendable @escaping (DatabaseOutput) -> Void
  ) async throws
  {
    do
    {
      let condition = try query.makeMongoDBFilter(aggregate: false)
      let count = CountCommand(on: query.schema, where: condition)

      logger.debug("fluent-mongo count condition=\(condition)")

      let counted = try await cluster.next(for: .writable)
        .executeCodable(
          count,
          decodeAs: CountCommand.self,
          namespace: MongoNamespace(to: "$cmd", inDatabase: raw.name),
          sessionId: nil
        )

      if let queried = counted.query
      {
        let reply = _MongoDBAggregateResponse(
          value: queried,
          decoder: BSONDecoder()
        )

        return onOutput(reply)
      }
    }
    catch
    {
      throw error
    }
  }

  private func group(
    query: DatabaseQuery,
    mongoOperator: String,
    field: DatabaseQuery.Field,
    onOutput: @Sendable @escaping (DatabaseOutput) -> Void
  ) async throws
  {
    do
    {
      let field = try field.makeMongoPath()
      let condition = try query.makeMongoDBFilter(aggregate: false)
      let find = raw[query.schema]
        .find(condition)
        .project([
          "n": [
            mongoOperator: "$\(field)",
          ],
        ])
      logger.debug("fluent-mongo find-group operation=\(mongoOperator) field=\(field) condition=\(condition)")

      try await find.firstResult().map
      { result in
        let res = _MongoDBAggregateResponse(
          value: result["n"] ?? Null(),
          decoder: BSONDecoder()
        )

        return onOutput(res)
      }
    }
    catch
    {
      throw error
    }
  }
}
