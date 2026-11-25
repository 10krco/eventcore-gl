import eventcore/projection
import gleam/int
import gleam/result
import gleam/string

const uuid_digits = 32

const max_uuid = 340_282_366_920_938_463_463_374_607_431_768_211_455

pub fn from_uuid(uuid: String) -> Result(projection.Position, Nil) {
  case string.split(uuid, "-") {
    [a, b, c, d, e] if a != "" && b != "" && c != "" && d != "" && e != "" -> {
      let digits = a <> b <> c <> d <> e
      case string.length(digits) == uuid_digits {
        False -> Error(Nil)
        True -> {
          use value <- result.try(int.base_parse(digits, 16))
          // Offset by one to represent the nil UUID in a positive cursor.
          case value >= 0 && value <= max_uuid {
            True -> {
              use position <- result.try(projection.position(value + 1))
              case to_uuid(position) == Ok(string.lowercase(uuid)) {
                True -> Ok(position)
                False -> Error(Nil)
              }
            }
            False -> Error(Nil)
          }
        }
      }
    }
    _ -> Error(Nil)
  }
}

pub fn to_uuid(position: projection.Position) -> Result(String, Nil) {
  let value = projection.sequence(position) - 1
  case value <= max_uuid {
    False -> Error(Nil)
    True -> {
      let hex = value |> int.to_base16 |> string.lowercase
      let digits = string.pad_start(hex, uuid_digits, "0")
      Ok(
        string.slice(digits, 0, 8)
        <> "-"
        <> string.slice(digits, 8, 4)
        <> "-"
        <> string.slice(digits, 12, 4)
        <> "-"
        <> string.slice(digits, 16, 4)
        <> "-"
        <> string.slice(digits, 20, 12),
      )
    }
  }
}
