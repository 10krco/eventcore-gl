import eventcore
import gleeunit

pub fn main() -> Nil {
  gleeunit.main()
}

pub fn upstream_release_test() {
  assert eventcore.upstream_release == "2.1.0"
}
