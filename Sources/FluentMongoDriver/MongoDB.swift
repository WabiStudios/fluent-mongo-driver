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

public extension DatabaseID
{
  static var mongo: DatabaseID
  {
    .init(string: "mongo")
  }
}

struct FluentMongoDatabase: Database, MongoDatabaseRepresentable
{
  let cluster: MongoCluster
  let raw: MongoDatabase
  let context: DatabaseContext
  let inTransaction: Bool

  func execute(
    query: DatabaseQuery,
    onOutput: @Sendable @escaping (DatabaseOutput) -> Void
  ) async throws
  {
    switch query.action
    {
      case .create:
        return try await create(query: query, onOutput: onOutput)
      case let .aggregate(aggregate):
        return try await self.aggregate(query: query, aggregate: aggregate, onOutput: onOutput)
      case .read where query.joins.isEmpty:
        return try await read(query: query, onOutput: onOutput)
      case .read:
        return try await join(query: query, onOutput: onOutput)
      case .update:
        return try await update(query: query, onOutput: onOutput)
      case .delete:
        return try await delete(query: query, onOutput: onOutput)
      case .custom:
        throw FluentMongoError.unsupportedCustomAction
    }
  }

  func execute(enum _: DatabaseEnum) -> EventLoopFuture<Void>
  {
    eventLoop.makeSucceededFuture(())
  }

  func withConnection<T>(_ closure: @escaping (Database) -> EventLoopFuture<T>) -> EventLoopFuture<T>
  {
    closure(self)
  }

  func execute(
    query: DatabaseQuery,
    onOutput: @escaping (DatabaseOutput) -> Void
  ) -> EventLoopFuture<Void>
  {
    switch query.action
    {
      case .create:
        return eventLoop.makeFutureWithTask
        { try await create(query: query) { output in onOutput(output) } }
        .transform(to: ())
      case let .aggregate(aggregate):
        return eventLoop.makeFutureWithTask
        { try await self.aggregate(query: query, aggregate: aggregate) { output in onOutput(output) } }
        .transform(to: ())
      case .read where query.joins.isEmpty:
        return eventLoop.makeFutureWithTask
        { try await read(query: query) { output in onOutput(output) } }
        .transform(to: ())
      case .read:
        return eventLoop.makeFutureWithTask
        { try await join(query: query) { output in onOutput(output) } }
        .transform(to: ())
      case .update:
        return eventLoop.makeFutureWithTask
        { try await update(query: query) { output in onOutput(output) } }
        .transform(to: ())
      case .delete:
        return eventLoop.makeFutureWithTask
        { try await delete(query: query) { output in onOutput(output) } }
        .transform(to: ())
      case .custom:
        return eventLoop.makeFailedFuture(FluentMongoError.unsupportedCustomAction)
    }
  }

  func transaction<T>(_ closure: @escaping (Database) -> EventLoopFuture<T>) -> EventLoopFuture<T>
  {
    eventLoop.makeFutureWithTask
    {
      try await transaction
      { trnsDB in
        try await closure(trnsDB).get()
      }
    }
  }
}

struct FluentMongoDriver: DatabaseDriver
{
  func makeDatabase(with context: DatabaseContext) -> Database
  {
    FluentMongoDatabase(
      cluster: cluster,
      raw: cluster[targetDatabase],
      context: context,
      inTransaction: false
    )
  }

  let cluster: MongoCluster
  let targetDatabase: String

  func shutdown()
  {
    Task.init
    {
      await cluster.disconnect()
    }
  }
}

public protocol MongoDatabaseRepresentable
{
  var raw: MongoDatabase { get }
}

struct FluentMongoConfiguration: DatabaseConfiguration
{
  let settings: ConnectionSettings
  let targetDatabase: String
  var middleware: [AnyModelMiddleware]

  func makeDriver(for _: Databases) -> DatabaseDriver
  {
    do
    {
      let cluster = try MongoCluster(lazyConnectingTo: settings)
      return FluentMongoDriver(
        cluster: cluster,
        targetDatabase: targetDatabase
      )
    }
    catch
    {
      fatalError("The MongoDB connection specification was malformed")
    }
  }
}

public extension DatabaseConfigurationFactory
{
  static func mongo(
    connectionString: String
  ) throws -> Self
  {
    try .mongo(settings: ConnectionSettings(connectionString))
  }

  static func mongo(
    settings: ConnectionSettings
  ) throws -> Self
  {
    guard !settings.hosts.isEmpty
    else
    {
      throw FluentMongoError.missingHosts
    }

    guard let targetDatabase = settings.targetDatabase
    else
    {
      throw FluentMongoError.noTargetDatabaseSpecified
    }

    return .init
    {
      FluentMongoConfiguration(
        settings: settings,
        targetDatabase:
        targetDatabase, middleware: []
      )
    }
  }
}
