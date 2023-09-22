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

extension DatabaseQuery
{
  func makeMongoDBSort() throws -> MongoKitten.Sort?
  {
    var sortSpec = [(String, SortOrder)]()

    for sort in sorts
    {
      switch sort
      {
        case let .sort(field, direction):
          let path = try field.makeMongoPath()
          try sortSpec.append((path, direction.makeMongoDirection()))
        case .custom:
          throw FluentMongoError.unsupportedCustomSort
      }
    }

    if sortSpec.isEmpty
    {
      return nil
    }

    let sorted: [(String, Sorting.Order)] = sortSpec.map
    {
      switch $0.1
      {
        case .forward:
          ($0.0, Sorting.Order.ascending)
        case .reverse:
          ($0.0, Sorting.Order.descending)
      }
    }
    return MongoKitten.Sort(Sorting(sorted))
  }

  func makeMongoDBFilter(aggregate: Bool) throws -> Document
  {
    var conditions = [Document]()

    for filter in filters
    {
      try conditions.append(filter.makeMongoDBFilter(aggregate: aggregate))
    }

    if conditions.isEmpty
    {
      return [:]
    }

    if conditions.count == 1
    {
      return conditions[0]
    }

    return AndQuery(conditions: conditions).makeDocument()
  }

  func makeValueDocuments() throws -> [Document]
  {
    try input.map
    { entity -> Document in
      try entity.makePrimitive() as! Document
    }
  }
}
