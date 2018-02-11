# mruby-unqlite : mruby bindings for unqlite (unofficial)

## HOW TO USAGE

```ruby:ruby
filepath = "sample.unqlite"
flags = UnQLite::CREATE
db = UnQLite.open(filepath, flags)

db.store("key", "value")
db.append("key", "+appended-value")

p ab.fetch("key") # => "value+appended-value"

db.close
```


## Specification

  * Package name: mruby-unqlite
  * Version: 0.1
  * Product quality: PROTOTYPE
  * Author: [dearblue](https://github.com/dearblue)
  * Project page: <https://github.com/dearblue/mruby-unqlite>
  * Licensing: [2 clause BSD License](LICENSE)
  * Dependency external mrbgems: (NONE)
  * Bundled C libraries (git submodules):
      * [unqlite-1.1.8](http://unqlite.org/)
        under [2-Clause BSD license](https://unqlite.org/licensing.html)
        by [Symisc Systems](https://www.symisc.net/)
