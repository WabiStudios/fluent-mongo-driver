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
  func create(
    query: DatabaseQuery,
    onOutput: @escaping (DatabaseOutput) -> Void
  ) -> EventLoopFuture<Void>
  {
    do
    {
      let documents = try query.makeValueDocuments()

      logger.debug("fluent-mongo insert entities=\(documents)")

      return eventLoop.makeFutureWithTask
      {
        let reply = try await raw[query.schema]
          .insertMany(documents)

        guard reply.ok == 1,
              reply.insertCount == documents.count
        else { throw FluentMongoError.insertFailed }

        let response = _MongoDBAggregateResponse(
          value: reply.insertCount,
          decoder: BSONDecoder()
        )

        onOutput(response)
      }
    }
    catch
    {
      return eventLoop.makeFailedFuture(error)
    }
  }
}
