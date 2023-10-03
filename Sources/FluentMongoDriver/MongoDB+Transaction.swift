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
    else { return closure(self) }

    let promise = eventLoop.makePromise(of: T.self)

    promise.completeWithTask
    {
      let transactionDatabase = try await raw.startTransaction(autoCommitChanges: false)
      let database = FluentMongoDatabase(
        cluster: cluster,
        raw: transactionDatabase,
        context: context,
        inTransaction: true
      )

      do
      {
        let result = try await closure(database).get()
        try await transactionDatabase.commit()
        return result
      }
      catch
      {
        try await transactionDatabase.abort()
        throw error
      }
    }

    return promise.futureResult
  }
}
