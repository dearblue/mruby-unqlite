#!ruby

class UnQLite
  def UnQLite.open(*args)
    db = new(*args)
    return db unless block_given?

    begin
      yield db
    ensure
      db.close
    end
  end

  alias [] fetch
  alias []= store

  def cursor
    cur = Cursor.new(self)

    return cur unless block_given?

    begin
      yield cur
    ensure
      cur.release
    end
  end

  def each
    return to_enum(:each) unless block_given?

    cursor { |cur| cur.each { yield cur } }

    self
  end

  def each_key
    return to_enum(:each_key) unless block_given?

    cursor { |cur| cur.each { yield cur.key } }

    self
  end

  def each_data
    return to_enum(:each_data) unless block_given?

    cursor { |cur| cur.each { yield cur.data } }

    self
  end

  alias each_value each_data

  def each_pair
    return to_enum(:each_pair) unless block_given?

    cursor { |cur| cur.each { yield cur.key, cur.data } }

    self
  end

  def transaction
    do_commit = true
    transaction_begin
  rescue Object
    do_commit = false
  ensure
    if do_commit
      transaction_commit
    else
      transaction_abort
    end
  end

  alias begin   transaction_begin
  alias commit  transaction_commit
  alias abort   transaction_abort

  class Cursor
    alias value data

    def each
      return to_enum(:each) unless block_given?

      reset

      while valid?
        yield self
        self.next
      end

      self
    end
  end
end

UNQLITE ||= UnQLite
Unqlite ||= UnQLite
