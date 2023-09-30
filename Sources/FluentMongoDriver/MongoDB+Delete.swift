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
  func delete(
    query: DatabaseQuery,
    onOutput: @Sendable @escaping (DatabaseOutput) -> Void
  ) async throws
  {
    do
    {
      let filter = try query.makeMongoDBFilter(aggregate: false)
      var deleteLimit: DeleteCommand.Limit = .all

      switch query.limits.first
      {
        case let .count(limit) where limit == 1:
          deleteLimit = .one
        case .custom, .count:
          throw FluentMongoError.unsupportedCustomLimit
        case .none:
          break
      }

      let command = DeleteCommand(
        where: filter,
        limit: deleteLimit,
        fromCollection: query.schema
      )

      logger.debug("fluent-mongo delete \(deleteLimit) filter=\(filter)")
      try await cluster.next(for: .writable)
        .executeCodable(
          command,
          decodeAs: DeleteCommand.self,
          namespace: MongoNamespace(to: "$cmd", inDatabase: raw.name),
          sessionId: nil
        )
        .deletes.forEach
        { deletes in
          let reply = _MongoDBAggregateResponse(
            value: deletes.query,
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
}
