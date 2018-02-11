#!ruby

MRuby::Build.new do |conf|
  toolchain :clang

  conf.build_dir = "host32"

  if cc.command =~ /\b(?:g?cc|clang)\d*\b/
    cc.flags << "-std=c11"
    cc.flags << "-pedantic"
    cc.flags << "-Wall"
  end

  cc.defines << "MRB_INT32"

  enable_debug
  enable_test

  gem core: "mruby-print"
  gem core: "mruby-bin-mrbc"
  gem core: "mruby-bin-mirb"
  gem core: "mruby-bin-mruby"
  gem File.dirname(__FILE__)
end

MRuby::Build.new("host-nan16") do |conf|
  toolchain :clang

  conf.build_dir = conf.name

  enable_debug
  enable_test

  cc.defines << "MRB_NAN_BOXING"
  cc.defines << "MRB_INT16"

  gem core: "mruby-print"
  gem core: "mruby-bin-mrbc"
  gem core: "mruby-bin-mruby"
  gem File.dirname(__FILE__)
end

MRuby::Build.new("host-word") do |conf|
  toolchain :clang

  conf.build_dir = conf.name

  cc.defines << "MRB_WORD_BOXING"
  cc.defines << "MRB_INT64" if [nil].pack("P").bytesize == 8

  enable_debug
  enable_test

  gem core: "mruby-print"
  gem core: "mruby-bin-mrbc"
  gem core: "mruby-bin-mruby"
  gem File.dirname(__FILE__)
end
