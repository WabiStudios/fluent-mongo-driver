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

extension DatabaseQuery.Sort.Direction
{
  func makeMongoDirection() throws -> Sorting.Order
  {
    switch self
    {
      case .ascending:
        return .ascending
      case .descending:
        return .descending
      case let .custom(order as Sorting.Order):
        return order
      case .custom:
        throw FluentMongoError.unsupportedCustomSort
    }
  }
}
