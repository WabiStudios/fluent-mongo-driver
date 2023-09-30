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
import MongoKitten

extension MongoDatabaseRepresentable
{
  var _gridFS: GridFSBucket
  {
    GridFSBucket(in: raw)
  }
}

public extension GridFSFile
{
  static func find(_ id: Primitive, on database: Database) async throws -> GridFSFile?
  {
    guard let mongodb = database as? MongoDatabaseRepresentable
    else { throw FluentMongoError.notMongoDB }

    return try await mongodb._gridFS.findFile(byId: id)
  }

  static func read(_ id: Primitive, on database: Database) async throws -> ByteBuffer?
  {
    guard let file = try await find(id, on: database)
    else { throw FluentMongoError.fileNotFound }

    return try await file.reader.readByteBuffer()
  }

  static func upload(
    _ buffer: ByteBuffer,
    named filename: String? = nil,
    metadata: Document? = nil,
    on database: Database
  ) async throws -> GridFSFile
  {
    guard let mongodb = database as? MongoDatabaseRepresentable
    else { throw FluentMongoError.notMongoDB }

    let writer = try await GridFSFileWriter(toBucket: mongodb._gridFS)
    try await writer.write(data: buffer)

    return try await writer.finalize(filename: filename, metadata: metadata)
  }
}
