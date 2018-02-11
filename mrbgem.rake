#!ruby

MRuby::Gem::Specification.new("mruby-unqlite") do |s|
  s.summary = "mruby bindings for unqlite"
  s.version = "0.1"
  s.license = "BSD-2-Clause"
  s.author  = "dearblue"
  s.homepage = "https://github.com/dearblue/mruby-unqlite"

  add_dependency "mruby-error",       core: "mruby-error"
  add_dependency "mruby-string-ext",  core: "mruby-string-ext"
  add_dependency "mruby-enumerator",  core: "mruby-enumerator"
  add_dependency "mruby-aux",         github: "dearblue/mruby-aux"

  if cc.command =~ /\bg?cc\d*\b/
    cc.flags << %w(
      -Wall
      -Wno-incompatible-pointer-types-discards-qualifiers
    )
  end

  dirp = dir.gsub(/[\[\]\{\}\,]/) { |m| "\\#{m}" }
  files = "contrib/unqlite/unqlite.c"
  objs.concat(Dir.glob(File.join(dirp, files)).map { |f|
    next nil unless File.file? f
    objfile f.relative_path_from(dir).pathmap("#{build_dir}/%X")
  }.compact)

  cc.include_paths.insert 0, File.join(dir, "contrib/unqlite")
end
