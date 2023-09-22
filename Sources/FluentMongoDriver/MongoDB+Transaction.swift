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
  func transaction<T>(_ closure: @escaping (Database) -> EventLoopFuture<T>) -> EventLoopFuture<T>
  {
    guard !inTransaction
    else
    {
      return closure(self)
    }

    return eventLoop.makeFutureWithTask
    {
      do
      {
        let transactionDatabase = try await raw.startTransaction(autoCommitChanges: false)
        let database = FluentMongoDatabase(
          cluster: self.cluster,
          raw: transactionDatabase,
          context: self.context,
          inTransaction: true
        )

        return try await closure(database).flatMap
        { value in

          eventLoop.makeFutureWithTask
          {
            try await transactionDatabase.commit()
            return value
          }
        }
        .flatMapError
        { error in
          eventLoop.makeFutureWithTask
          {
            try await transactionDatabase.abort()
            throw error
          }
        }
        .get()
      }
      catch
      {
        return try await eventLoop.makeFailedFuture(error).get()
      }
    }
  }
}
