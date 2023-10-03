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
  func update(
    query: DatabaseQuery,
    onOutput: @escaping (DatabaseOutput) -> Void
  ) async throws
  {
    do
    {
      let filter = try query.makeMongoDBFilter(aggregate: false)
      let update = try query.makeValueDocuments()

      let updates = update.map
      { document in
        var update = UpdateCommand.UpdateRequest(
          where: filter,
          to: [
            "$set": document,
          ]
        )

        update.multi = true

        return update
      }

      let command = UpdateCommand(updates: updates, inCollection: query.schema)
      logger.debug("fluent-mongo update filter=\(filter) updates=\(update)")
      let modified = try await cluster.next(for: .writable).executeCodable(
        command,
        decodeAs: UpdateReply.self,
        namespace: MongoNamespace(to: "$cmd", inDatabase: raw.name),
        sessionId: nil
      )

      let reply = _MongoDBAggregateResponse(
        value: modified.updatedCount,
        decoder: BSONDecoder()
      )

      return onOutput(reply)
    }
    catch
    {
      throw error
    }
  }
}
