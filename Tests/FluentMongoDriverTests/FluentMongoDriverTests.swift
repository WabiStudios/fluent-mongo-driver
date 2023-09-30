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

import FluentBenchmark
import FluentMongoDriver
import Logging
import MongoKitten
import NIO
import XCTest

final class DateRange: Model
{
  static let schema = "date-range"

  @ID(key: .id)
  var id: UUID?

  @Field(key: "start")
  var start: Date

  @Field(key: "end")
  var end: Date

  init() {}

  init(from: Date, to: Date)
  {
    start = from
    end = to
  }
}

public final class Entity: Model
{
  public static let schema = "entities"

  @ID(custom: .id)
  public var id: ObjectId?

  @Field(key: "name")
  public var name: String

  public init() {}

  public init(id: ObjectId? = nil, name: String)
  {
    self.id = id
    self.name = name
  }
}

public final class DocumentStorage: Model
{
  public static let schema = "documentstorages"

  @ID(custom: .id)
  public var id: ObjectId?

  @Field(key: "document")
  public var document: Document

  public init() {}

  public init(id: ObjectId? = nil, document: Document)
  {
    self.id = id
    self.document = document
  }
}

public final class Nested: Fields
{
  @Field(key: "value")
  public var value: String

  public init() {}
  public init(value: String)
  {
    self.value = value
  }
}

public final class NestedStorage: Model
{
  public static let schema = "documentstorages"

  @ID(custom: .id)
  public var id: ObjectId?

  @Field(key: "nested")
  public var nested: Nested

  public init() {}

  public init(id: ObjectId? = nil, nested: Nested)
  {
    self.id = id
    self.nested = nested
  }
}

final class FluentMongoDriverTests: XCTestCase
{
  func testAggregate() throws { try benchmarker.testAggregate(max: false) }
  func testArray() throws { try benchmarker.testArray() }
  func testBatch() throws { try benchmarker.testBatch() }
  func testChildren() throws { try benchmarker.testChildren() }
  func testChunk() throws { try benchmarker.testChunk() }
  func testCompositeID() throws { try benchmarker.testCompositeID() }
  func testCRUD() throws { try benchmarker.testCRUD() }
  func testEagerLoad() throws { try benchmarker.testEagerLoad() }
  func testEnum() throws { try benchmarker.testEnum() }
  func testGroup() throws { try benchmarker.testGroup() }
  func testID() throws
  {
    try benchmarker.testID(
      autoincrement: false,
      custom: false
    )
  }

  func testFilter() throws { try benchmarker.testFilter(sql: false) }
  func testJoin() throws { try benchmarker.testJoin() }
  func testMiddleware() throws { try benchmarker.testMiddleware() }
  func testMigrator() throws { try benchmarker.testMigrator() }
  func testModel() throws { try benchmarker.testModel() }
  func testOptionalParent() throws { try benchmarker.testOptionalParent() }
  func testPagination() throws { try benchmarker.testPagination() }
  func testParent() throws { try benchmarker.testParent() }
  func testPerformance() throws { try benchmarker.testPerformance() }
  func testRange() throws { try benchmarker.testRange() }
  func testSet() throws { try benchmarker.testSet() }
  func testSiblings() throws { try benchmarker.testSiblings() }
  func testSoftDelete() throws { try benchmarker.testSoftDelete() }
  func testSort() throws { try benchmarker.testSort(sql: false) }
  func testTimestamp() throws { try benchmarker.testTimestamp() }
  func testUnique() throws { try benchmarker.testUnique() }

  func testJoinLimit() throws
  {
    let migration = SolarSystem()
    try migration.prepare(on: db).wait()
    defer
    {
      _ = try? migration.revert(on: db).wait()
    }

    do
    {
      let planets = try Planet.query(on: db).all().wait()

      guard planets.count > 1, let lastId = planets.last?.id
      else
      {
        XCTFail("Invalid dataset for test")
        return
      }

      let planet = try Planet.query(on: db)
        .join(Star.self, on: \Planet.$star.$id == \Star.$id)
        .filter(\.$id == lastId)
        .first()
        .wait()

      XCTAssertEqual(planet?.id, lastId)
    }
    catch
    {
      XCTFail("\(error)")
    }
  }

  func testDate() throws
  {
    let range = DateRange(from: Date(), to: Date())
    try range.save(on: db).wait()

    guard let sameRange = try DateRange.find(range.id, on: db).wait()
    else
    {
      XCTFail()
      return
    }

    // Dates are doubles, which are not 100% precise. So this fails on Linux.
    XCTAssert(abs(range.start.timeIntervalSince(sameRange.start)) < 0.1)
    XCTAssert(abs(range.end.timeIntervalSince(sameRange.end)) < 0.1)
  }

  func testNestedDocuments() throws
  {
    let doc = DocumentStorage(document: ["key": true])
    try doc.save(on: db).wait()

    guard let sameDoc = try DocumentStorage.query(on: db).filter("document.key", .equal, true).first().wait()
    else
    {
      XCTFail("Query failed to find the saved entity")
      return
    }

    XCTAssertEqual(sameDoc.document["key"] as? Bool, true)
  }

  func testNestedFields() throws
  {
    let doc = NestedStorage(nested: .init(value: "hello"))
    try doc.save(on: db).wait()

    guard let sameDoc = try NestedStorage.query(on: db).filter("nested.value", .equal, "hello").first().wait()
    else
    {
      XCTFail("Query failed to find the saved entity")
      return
    }

    XCTAssertEqual(sameDoc.nested.value, "hello")
  }

  func testObjectId() throws
  {
    let entity = Entity(name: "test")

    XCTAssertEqual(try Entity.query(on: db).count().wait(), 0)

    try entity.save(on: db).wait()
    XCTAssertEqual(try Entity.query(on: db).count().wait(), 1)

    XCTAssertNotNil(try Entity.find(entity.id, on: db).wait())

    try entity.delete(on: db).wait()
    XCTAssertEqual(try Entity.query(on: db).count().wait(), 0)
  }

  func testGridFS() async throws
  {
    struct JSON: Codable, Equatable
    {
      let name: String
    }

    let writtenEntity = JSON(name: "Hello")
    let writtenData = try JSONEncoder().encode(writtenEntity)
    var buffer = ByteBufferAllocator().buffer(capacity: writtenData.count)
    buffer.writeBytes(writtenData)

    let writtenFile = try await GridFSFile.upload(buffer, on: db)

    guard let readBuffer = try await GridFSFile.read(writtenFile._id, on: db)
    else { XCTFail("File not found"); return }

    guard let readBytes = readBuffer.getBytes(at: 0, length: writtenData.count)
    else { XCTFail("Mismatching data"); return }

    let readEntity = try JSONDecoder().decode(JSON.self, from: Data(readBytes))
    XCTAssertEqual(writtenEntity, readEntity)
  }

  var benchmarker: FluentBenchmarker
  {
    .init(databases: dbs)
  }

  var eventLoopGroup: EventLoopGroup!
  var threadPool: NIOThreadPool!
  var dbs: Databases!
  var db: Database
  {
    benchmarker.database
  }

  var mongodb: MongoDatabaseRepresentable
  {
    db as! MongoDatabaseRepresentable
  }

  override func setUpWithError() throws
  {
    try super.setUpWithError()

    XCTAssert(isLoggingConfigured)
    eventLoopGroup = MultiThreadedEventLoopGroup(numberOfThreads: System.coreCount)
    threadPool = NIOThreadPool(numberOfThreads: System.coreCount)
    dbs = Databases(threadPool: threadPool, on: eventLoopGroup)

    try dbs.use(.mongo(settings: .init(
      authentication: .unauthenticated,
      hosts: [.init(
        hostname: env("MONGO_HOSTNAME_A") ?? "localhost",
        port: env("MONGO_PORT_A").flatMap(Int.init) ?? 27017
      )],
      targetDatabase: env("MONGO_DATABASE_A") ?? "test_database"
    )), as: .a)
    try dbs.use(.mongo(settings: .init(
      authentication: .unauthenticated,
      hosts: [.init(
        hostname: env("MONGO_HOSTNAME_B") ?? "localhost",
        port: env("MONGO_PORT_B").flatMap(Int.init) ?? 27017
      )],
      targetDatabase: env("MONGO_DATABASE_B") ?? "test_database"
    )), as: .b)

    // Drop existing tables.
    let a = dbs.database(.a, logger: Logger(label: "test.fluent.a"), on: eventLoopGroup.any()) as! MongoDatabaseRepresentable
    try dbs.eventLoopGroup.makeFutureWithTask
    {
      try await a.raw.drop()
    }.wait()

    let b = dbs.database(.b, logger: Logger(label: "test.fluent.b"), on: eventLoopGroup.any()) as! MongoDatabaseRepresentable
    try dbs.eventLoopGroup.makeFutureWithTask
    {
      try await b.raw.drop()
    }.wait()
  }

  override func tearDownWithError() throws
  {
    dbs.shutdown()
    try threadPool.syncShutdownGracefully()
    try eventLoopGroup.syncShutdownGracefully()

    try super.tearDownWithError()
  }
}

func env(_ name: String) -> String?
{
  ProcessInfo.processInfo.environment[name]
}

let isLoggingConfigured: Bool = {
  LoggingSystem.bootstrap
  { label in
    var handler = StreamLogHandler.standardOutput(label: label)
    handler.logLevel = env("LOG_LEVEL").flatMap { Logger.Level(rawValue: $0) } ?? .debug
    return handler
  }
  return true
}()

extension DatabaseID
{
  static let a = DatabaseID(string: "mongo-a")
  static let b = DatabaseID(string: "mongo-b")
}
