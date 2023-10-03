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

struct _MongoDBAggregateResponse: DatabaseOutput
{
  let value: Primitive
  let decoder: BSONDecoder

  var description: String
  {
    "\(value)"
  }

  func schema(_: String) -> DatabaseOutput
  {
    self
  }

  func contains(_ key: FieldKey) -> Bool
  {
    key == .aggregate
  }

  func decodeNil(_: FieldKey) throws -> Bool
  {
    false
  }

  func decode<T>(_: FieldKey, as type: T.Type) throws -> T
    where T: Decodable
  {
    try decoder.decode(type, fromPrimitive: value)
  }
}
