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
  func transaction<T>(
    _ closure: @Sendable @escaping (Database) async throws -> T
  ) async throws -> T
  {
    guard !inTransaction
    else { return try await closure(self) }

    do
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
        let transacted = try await closure(database)
        try await transactionDatabase.commit()
        return transacted
      }
      catch
      {
        try await transactionDatabase.abort()
        throw error
      }
    }
    catch
    {
      throw error
    }
  }
}
