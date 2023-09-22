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
import Foundation
import MongoCore
import MongoKitten

extension FluentMongoDatabase
{
  func execute(schema: DatabaseSchema) -> EventLoopFuture<Void>
  {
    switch schema.action
    {
      case .create, .update:
        return update(schema: schema)
      case .delete:
        return delete(schema: schema)
    }
  }

  private func update(schema: DatabaseSchema) -> EventLoopFuture<Void>
  {
    do
    {
      var futures = [EventLoopFuture<Void>]()

      nextConstraint: for constraint in schema.createConstraints
      {
        guard case let .constraint(algorithm, _) = constraint
        else
        {
          continue nextConstraint
        }
        switch algorithm
        {
          case let .unique(fields), let .compositeIdentifier(fields):
            let indexKeys = try fields.map
            { field -> String in
              switch field
              {
                case let .key(key):
                  return key.makeMongoKey()
                case .custom:
                  throw FluentMongoError.invalidIndexKey
              }
            }

            var keys = Document()

            for key in indexKeys
            {
              keys[key] = SortOrder.forward.hashValue
            }

            var index = CreateIndexes.Index(
              named: "unique",
              keys: keys
            )

            index.unique = true

            let createIndexes = CreateIndexes(
              collection: schema.schema,
              indexes: [index]
            )

            let createdIndex = eventLoop.makeFutureWithTask
            {
              let _ = try await cluster.next(for: .writable)
                .executeCodable(
                  createIndexes,
                  decodeAs: InsertCommand.self,
                  namespace: MongoNamespace(to: "$cmd", inDatabase: raw.name),
                  sessionId: nil
                )
            }

            futures.append(createdIndex)
          case .foreignKey, .custom:
            continue nextConstraint
        }
      }

      return EventLoopFuture.andAllSucceed(futures, on: eventLoop)
    }
    catch
    {
      return eventLoop.makeFailedFuture(error)
    }
  }

  private func delete(schema: DatabaseSchema) -> EventLoopFuture<Void>
  {
    eventLoop.makeFutureWithTask
    {
      try await raw[schema.schema].drop()
    }
  }
}
