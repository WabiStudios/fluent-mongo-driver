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
  func join(
    query: DatabaseQuery,
    onOutput: @Sendable @escaping (DatabaseOutput) -> Void
  ) async throws
  {
    do
    {
      let stages = try query.makeAggregatePipeline()
      let decoder = BSONDecoder()
      logger.debug("fluent-mongo join stages=\(stages)")

      AggregateBuilderPipeline(stages: stages, collection: raw[query.schema]).forEach
      { document in
        onOutput(document.databaseOutput(using: decoder))
      }
    }
    catch
    {
      throw error
    }
  }

  func joinCount(
    query: DatabaseQuery,
    onOutput: @Sendable @escaping (DatabaseOutput) -> Void
  ) async throws
  {
    do
    {
      let stages = try query.makeAggregatePipeline()
      logger.debug("fluent-mongo join-count stages=\(stages)")
      let count = try await AggregateBuilderPipeline(stages: stages, collection: raw[query.schema]).count()
      let reply = _MongoDBAggregateResponse(value: count, decoder: BSONDecoder())
      return onOutput(reply)
    }
    catch
    {
      throw error
    }
  }
}

extension DatabaseQuery
{
  func makeAggregatePipeline() throws -> [AggregateBuilderStage]
  {
    var stages = [AggregateBuilderStage]()

    stages.append(ReplaceRoot(with: "$$ROOT"))

    for join in joins
    {
      switch join
      {
        case let .join(schema, alias, method, foreignKey, localKey):
          switch method
          {
            case .left:
              try stages.append(Lookup(
                from: schema,
                localField: FieldPath(stringLiteral: localKey.makeProjectedMongoPath()),
                foreignField: FieldPath(stringLiteral: foreignKey.makeMongoPath()),
                as: FieldPath(stringLiteral: alias ?? schema)
              ))
            case .inner:
              try stages.append(Lookup(
                from: schema,
                localField: FieldPath(stringLiteral: localKey.makeProjectedMongoPath()),
                foreignField: FieldPath(stringLiteral: foreignKey.makeMongoPath()),
                as: FieldPath(stringLiteral: alias ?? schema)
              ))

              stages.append(Unwind(fieldPath: "$unwind", includeArrayIndex: "$\(alias ?? schema)"))
            case .custom:
              throw FluentMongoError.unsupportedJoin
          }
        case let .extendedJoin(schema, space, alias, method, foreignKey, localKey):
          guard space == nil else { throw FluentMongoError.unsupportedJoin }
          switch method
          {
            case .left:
              try stages.append(Lookup(
                from: schema,
                localField: FieldPath(stringLiteral: localKey.makeProjectedMongoPath()),
                foreignField: FieldPath(stringLiteral: foreignKey.makeMongoPath()),
                as: FieldPath(stringLiteral: alias ?? schema)
              ))
            case .inner:
              try stages.append(Lookup(
                from: schema,
                localField: FieldPath(stringLiteral: localKey.makeProjectedMongoPath()),
                foreignField: FieldPath(stringLiteral: foreignKey.makeMongoPath()),
                as: FieldPath(stringLiteral: alias ?? schema)
              ))

              stages.append(Unwind(fieldPath: "$unwind", includeArrayIndex: "$\(alias ?? schema)"))
            case .custom:
              throw FluentMongoError.unsupportedJoin
          }
        case .advancedJoin(let schema, nil, let alias, let method, let filters) where filters.count == 1:
          guard case let .field(lKey, fMethod, fKey) = filters[0], case .equality(inverse: false) = fMethod
          else
          {
            throw FluentMongoError.unsupportedJoin
          }
          switch method
          {
            case .left:
              try stages.append(Lookup(from: schema,
                                       localField: FieldPath(stringLiteral: lKey.makeProjectedMongoPath()),
                                       foreignField: FieldPath(stringLiteral: fKey.makeMongoPath()),
                                       as: FieldPath(stringLiteral: alias ?? schema)))
            case .inner:
              try stages.append(Lookup(from: schema,
                                       localField: FieldPath(stringLiteral: lKey.makeProjectedMongoPath()),
                                       foreignField: FieldPath(stringLiteral: fKey.makeMongoPath()),
                                       as: FieldPath(stringLiteral: alias ?? schema)))
              stages.append(Unwind(fieldPath: "$unwind", includeArrayIndex: "$\(alias ?? schema)"))
            case .custom:
              throw FluentMongoError.unsupportedJoin
          }
        case .advancedJoin:
          throw FluentMongoError.unsupportedJoin
        case .custom:
          throw FluentMongoError.unsupportedJoin
      }
    }

    let filter = try makeMongoDBFilter(aggregate: true)

    if !filter.isEmpty
    {
      stages.append(Match(where: filter))
    }

    switch offsets.first
    {
      case let .count(offset):
        stages.append(Skip(offset))
      case .custom:
        throw FluentMongoError.unsupportedCustomLimit
      case .none:
        break
    }

    switch limits.first
    {
      case let .count(n):
        stages.append(MongoKitten.Limit(n))
      case .custom:
        throw FluentMongoError.unsupportedCustomLimit
      case .none:
        break
    }

    var projection = Projection(document: [:])
    for field in fields
    {
      try projection.include(FieldPath(stringLiteral: field.makeProjectedMongoPath()))
    }
    stages.append(Project(projection: projection))

    return stages
  }
}
