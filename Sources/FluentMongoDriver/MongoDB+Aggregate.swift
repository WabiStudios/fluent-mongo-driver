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
    onOutput: @escaping (DatabaseOutput) -> Void
  ) -> EventLoopFuture<Void>
  {
    guard case let .field(field, method) = aggregate
    else
    {
      return eventLoop.makeFailedFuture(FluentMongoError.unsupportedCustomAggregate)
    }

    switch method
    {
      case .count where query.joins.isEmpty:
        return count(query: query, onOutput: onOutput)
      case .count:
        return joinCount(query: query, onOutput: onOutput)
      case .sum:
        return group(
          query: query,
          mongoOperator: "$sum",
          field: field,
          onOutput: onOutput
        )
      case .average:
        return group(
          query: query,
          mongoOperator: "$avg",
          field: field,
          onOutput: onOutput
        )
      case .maximum:
        return group(
          query: query,
          mongoOperator: "$max",
          field: field,
          onOutput: onOutput
        )
      case .minimum:
        return group(
          query: query,
          mongoOperator: "$min",
          field: field,
          onOutput: onOutput
        )
      case .custom:
        return eventLoop.makeFailedFuture(FluentMongoError.unsupportedCustomAggregate)
    }
  }

  private func count(
    query: DatabaseQuery,
    onOutput: @escaping (DatabaseOutput) -> Void
  ) -> EventLoopFuture<Void>
  {
    do
    {
      let condition = try query.makeMongoDBFilter(aggregate: false)
      let count = CountCommand(on: query.schema, where: condition)

      logger.debug("fluent-mongo count condition=\(condition)")

      return eventLoop.makeFutureWithTask
      {
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
    }
    catch
    {
      return eventLoop.makeFailedFuture(error)
    }
  }

  private func group(
    query: DatabaseQuery,
    mongoOperator: String,
    field: DatabaseQuery.Field,
    onOutput: @escaping (DatabaseOutput) -> Void
  ) -> EventLoopFuture<Void>
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

      return eventLoop.makeFutureWithTask
      {
        try await find.firstResult().map
        { result in
          let res = _MongoDBAggregateResponse(
            value: result["n"] ?? Null(),
            decoder: BSONDecoder()
          )

          onOutput(res)
        }
      }
    }
    catch
    {
      return eventLoop.makeFailedFuture(error)
    }
  }
}
