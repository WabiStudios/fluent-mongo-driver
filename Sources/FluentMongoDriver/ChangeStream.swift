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
import MongoKitten

public extension Model
{
  static func watch(on database: Database, options: ChangeStreamOptions = .init()) -> EventLoopFuture<ChangeStream<Self>>
  {
    guard let mongodb = database as? MongoDatabaseRepresentable
    else
    {
      return database.eventLoop.makeFailedFuture(FluentMongoError.notMongoDB)
    }

    return database.eventLoop.makeFutureWithTask
    {
      try await mongodb.raw[Self.schema].watch(options: options, type: Self.self)
    }
  }
}
