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
    let promise = eventLoop.makePromise(of: Void.self)

    switch schema.action
    {
      case .create, .update:
        promise.completeWithTask
        {
          try await update(schema: schema)
        }
      case .delete:
        promise.completeWithTask
        {
          try await delete(schema: schema)
        }
    }

    return promise.futureResult
  }

  private func update(schema: DatabaseSchema) async throws
  {
    do
    {
      var schematic = [MongoServerReply]()

      nextConstraint: for constraint in schema.createConstraints
      {
        guard case let .constraint(algorithm, _) = constraint
        else { continue nextConstraint }

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
              keys[key] = Sorting.Order.ascending.rawValue
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

            let createdIndex = try await cluster.next(for: .writable).executeEncodable(
              createIndexes,
              namespace: MongoNamespace(to: "$cmd", inDatabase: raw.name),
              sessionId: nil
            )

            schematic.append(createdIndex)
          case .foreignKey, .custom:
            continue nextConstraint
        }
      }

      try schematic.forEach
      { scheme in

        try scheme.assertOK()
      }
    }
    catch
    {
      throw error
    }
  }

  private func delete(schema: DatabaseSchema) async throws
  {
    try await raw[schema.schema].drop()
  }
}
