#include <mruby.h>
#include <mruby/class.h>
#include <mruby/variable.h>
#include <mruby/array.h>
#include <mruby/hash.h>
#include <mruby/string.h>
#include <mruby/value.h>
#include <mruby/data.h>
#include <mruby/error.h>
#include <unqlite.h>
#include <stdlib.h>
#include <mruby-aux.h>
#include <mruby-aux/string/hexdigest.h>

#define NOT_REACHED_HERE                                \
    mrb_bug(mrb, "MUST BE NOT REACHED HERE (%S:%S:%S)", \
            mrb_str_new_lit(mrb, __FILE__),             \
            mrb_fixnum_value(__LINE__),                 \
            mrb_str_new_cstr(mrb, __func__))            \

#define id_initialize   mrb_intern_lit(mrb, "initialize")

#ifndef MRUBY_UNQLITE_DEFAULT_FETCH_SIZE
#   ifdef MRB_INT16
                                                /* 4 KiB */
#       define MRUBY_UNQLITE_DEFAULT_FETCH_SIZE (4 << 10)
#   else
                                                /* 1 MiB */
#       define MRUBY_UNQLITE_DEFAULT_FETCH_SIZE (1 << 20)
#   endif
#endif


static const char *
aux_error_message(int err)
{
    /* NOTE: テキストメッセージは https://www.unqlite.org/c_api_const.html から拝借 */

    switch (err) {
    case UNQLITE_OK:            return "UNQLITE_OK - Successful result";
    case UNQLITE_NOMEM:         return "UNQLITE_NOMEM - Out of memory";
    case UNQLITE_ABORT:         return "UNQLITE_ABORT - Another thread have released this instance";
    case UNQLITE_IOERR:         return "UNQLITE_IOERR - IO error";
    case UNQLITE_CORRUPT:       return "UNQLITE_CORRUPT - Corrupt pointer";
    case UNQLITE_LOCKED:        return "UNQLITE_LOCKED - Forbidden Operation";
    case UNQLITE_BUSY:          return "UNQLITE_BUSY - The database file is locked";
    case UNQLITE_DONE:          return "UNQLITE_DONE - Operation done";
    case UNQLITE_PERM:          return "UNQLITE_PERM - Permission error";
    case UNQLITE_NOTIMPLEMENTED: return "UNQLITE_NOTIMPLEMENTED - Method not implemented by the underlying Key/Value storage engine";
    case UNQLITE_NOTFOUND:      return "UNQLITE_NOTFOUND - No such record";
    case UNQLITE_NOOP:          return "UNQLITE_NOOP - No such method";
    case UNQLITE_INVALID:       return "UNQLITE_INVALID - Invalid parameter";
    case UNQLITE_EOF:           return "UNQLITE_EOF - End Of Input";
    case UNQLITE_UNKNOWN:       return "UNQLITE_UNKNOWN - Unknown configuration option";
    case UNQLITE_LIMIT:         return "UNQLITE_LIMIT - Database limit reached";
    case UNQLITE_EXISTS:        return "UNQLITE_EXISTS - Records exists";
    case UNQLITE_EMPTY:         return "UNQLITE_EMPTY - Empty record";
    case UNQLITE_COMPILE_ERR:   return "UNQLITE_COMPILE_ERR - Compilation error";
    case UNQLITE_VM_ERR:        return "UNQLITE_VM_ERR - Virtual machine error";
    case UNQLITE_FULL:          return "UNQLITE_FULL - Full database (unlikely)";
    case UNQLITE_CANTOPEN:      return "UNQLITE_CANTOPEN - Unable to open the database file";
    case UNQLITE_READ_ONLY:     return "UNQLITE_READ_ONLY - Read only Key/Value storage engine";
    case UNQLITE_LOCKERR:       return "UNQLITE_LOCKERR - Locking protocol error";
    default:                    return "unknown";
    }
}

static void
aux_check_error(MRB, int err, const char *mesg)
{
    if (err == UNQLITE_OK) { return; }

    mrb_raisef(mrb, E_RUNTIME_ERROR,
               "%S failed - %S (0x%S)",
               mrb_str_new_cstr(mrb, mesg),
               mrb_str_new_cstr(mrb, aux_error_message(err)),
               mrbx_str_new_as_hexdigest(mrb, err, 4));
}

static int
aux_convert_seek_pos(MRB, VALUE pos)
{
    if (NIL_P(pos)) {
        return UNQLITE_CURSOR_MATCH_EXACT;
    } else if (mrb_string_p(pos) || mrb_symbol_p(pos)) {
        const char *str = mrbx_get_const_cstr(mrb, pos);

        if (strcasecmp(str, "exact") == 0 || strcasecmp(str, "match_exact") == 0) {
            return UNQLITE_CURSOR_MATCH_EXACT;
        } else if (strcasecmp(str, "le") == 0 || strcasecmp(str, "match_le") == 0) {
            return UNQLITE_CURSOR_MATCH_LE;
        } else if (strcasecmp(str, "ge") == 0 || strcasecmp(str, "match_ge") == 0) {
            return UNQLITE_CURSOR_MATCH_GE;
        }

        mrb_raisef(mrb, E_ARGUMENT_ERROR,
                   "wrong cursor match pos - %S (expect exact, le, ge or nil",
                   pos);
    }

    return mrb_int(mrb, pos);
}


/*
 * class UnQLite
 */

struct context
{
    unqlite *db;
};

static void
ext_free(MRB, struct context *p)
{
    if (p->db) {
        unqlite_close(p->db);
        p->db = NULL;
    }

    mrb_free(mrb, p);
}

static const mrb_data_type mruby_unqlite_type = {
    "context@mruby-unqlite",
    (void (*)(MRB, void *))ext_free,
};

static struct context *
getcontext(MRB, mrb_value obj)
{
    return (struct context *)mrb_data_get_ptr(mrb, obj, &mruby_unqlite_type);
}

static mrb_value
ext_s_new(MRB, mrb_value self)
{
    struct RData *data = mrb_data_object_alloc(mrb, mrb_class_ptr(self), NULL, &mruby_unqlite_type);
    data->data = mrb_calloc(mrb, 1, sizeof(struct context));

    mrb_value obj = mrb_obj_value(data);
    mrbx_funcall_passthrough(mrb, obj, id_initialize);

    return obj;
}

/*
 * call-seq:
 *  initialize(path, mode = UnQLite::CREATE | UnQLite::READWRITE)
 *
 * [path (string OR nil)]
 *
 * [mode (integer)]
 *  See https://unqlite.org/c_api/unqlite_open.html .
 */
static mrb_value
ext_initialize(MRB, mrb_value self)
{
    const char *path;
    mrb_int mode;

    switch (mrb_get_args(mrb, "z!|i", &path, &mode)) {
    case 1:
        mode = UNQLITE_OPEN_CREATE | UNQLITE_OPEN_READWRITE;
        break;
    case 2:
        /* do nothing */
        break;
    default:
        NOT_REACHED_HERE;
    }

    struct context *p = getcontext(mrb, self);
    int rc = unqlite_open(&p->db, path, mode);
    aux_check_error(mrb, rc, "unqlite_open");

    return self;
}

/*
 * call-seq:
 *  store(key, value) -> self
 */
static mrb_value
ext_store(MRB, mrb_value self)
{
    struct context *p = getcontext(mrb, self);
    const char *key, *value;
    mrb_int keylen, valuelen;
    mrb_get_args(mrb, "ss", &key, &keylen, &value, &valuelen);

    int rc = unqlite_kv_store(p->db, key, keylen, value, valuelen);
    aux_check_error(mrb, rc, "unqlite_kv_store");

    return self;
}

/*
 * call-seq:
 *  append(key, value) -> self
 */
static mrb_value
ext_append(MRB, mrb_value self)
{
    struct context *p = getcontext(mrb, self);
    const char *key, *value;
    mrb_int keylen, valuelen;
    mrb_get_args(mrb, "ss", &key, &keylen, &value, &valuelen);

    int rc = unqlite_kv_append(p->db, key, keylen, value, valuelen);
    aux_check_error(mrb, rc, "unqlite_kv_append");

    return self;
}

/*
 * call-seq:
 *  size(key) -> dest
 */
static mrb_value
ext_size(MRB, mrb_value self)
{
    struct context *p = getcontext(mrb, self);
    mrb_value key;
    mrb_get_args(mrb, "S", &key);

    unqlite_int64 size;
    int rc = unqlite_kv_fetch(p->db, RSTRING_PTR(key), RSTRING_LEN(key), NULL, &size);
    if (rc == UNQLITE_NOTFOUND) { return Qnil; }
    aux_check_error(mrb, rc, "unqlite_kv_fetch");

    if (size > MRB_INT_MAX) {
        return mrb_float_value(mrb, size);
    } else {
        return mrb_fixnum_value(size);
    }
}

static void
ext_fetch_args(MRB, VALUE self, struct context **p, struct RString **key, unqlite_int64 *maxvalue, struct RString **value)
{
    mrb_int argc;
    mrb_value *argv;

    switch (mrb_get_args(mrb, "*", &argv, &argc)) {
    case 1:
        *maxvalue = -1;
        *value = NULL;
        break;
    case 2:
        if (mrb_string_p(argv[1])) {
            *maxvalue = -1;
            *value = RSTRING(argv[1]);
        } else {
            *maxvalue = (NIL_P(argv[1]) ? -1 : mrb_int(mrb, argv[1]));
            *value = NULL;
        }
        break;
    case 3:
        *maxvalue = (NIL_P(argv[1]) ? -1 : mrb_int(mrb, argv[1]));
        *value = RString(argv[2]);
        break;
    default:
        mrb_raisef(mrb, E_ARGUMENT_ERROR,
                   "wrong number for arguments (given %S, expect 1..3)",
                   mrb_fixnum_value(argc));
    }

    mrb_check_type(mrb, argv[0], MRB_TT_STRING);
    *key = RSTRING(argv[0]);

    *p = getcontext(mrb, self);

    if (*maxvalue < 0) {
        unqlite_int64 size = 0;
        unqlite_kv_fetch((*p)->db, RSTR_PTR(*key), RSTR_LEN(*key), NULL, &size);
        if (size > MRBX_STR_MAX) {
            *maxvalue = MRBX_STR_MAX;
        } else {
            *maxvalue = size;
        }
    }

    *value = mrbx_str_force_recycle(mrb, *value, *maxvalue);
}

/*
 * call-seq:
 *  fetch(key, maxsize = nil, buf = "") -> buf
 *  fetch(key, buf) -> buf
 */
static mrb_value
ext_fetch(MRB, mrb_value self)
{
    struct context *p;
    struct RString *key, *value;
    unqlite_int64 maxvalue;
    ext_fetch_args(mrb, self, &p, &key, &maxvalue, &value);

    int rc = unqlite_kv_fetch(p->db, RSTR_PTR(key), RSTR_LEN(key), RSTR_PTR(value), &maxvalue);
    if (rc == UNQLITE_NOTFOUND) { return Qnil; }
    aux_check_error(mrb, rc, "unqlite_kv_fetch");
    mrbx_str_set_len(mrb, value, maxvalue);

    return VALUE(value);
}

static mrb_value
ext_delete(MRB, mrb_value self)
{
    struct context *p = getcontext(mrb, self);
    mrb_value key;
    mrb_get_args(mrb, "S", &key);

    int rc = unqlite_kv_delete(p->db, RSTRING_PTR(key), RSTRING_LEN(key));
    aux_check_error(mrb, rc, "unqlite_kv_delete");

    return Qnil;
}

static mrb_value
ext_close(MRB, mrb_value self)
{
    struct context *p = getcontext(mrb, self);

    unqlite_close(p->db);
    p->db = NULL;

    return Qnil;
}

static mrb_value
ext_begin(MRB, mrb_value self)
{
    int rc = unqlite_begin(getcontext(mrb, self)->db);
    aux_check_error(mrb, rc, "unqlite_begin");

    return self;
}

static mrb_value
ext_commit(MRB, mrb_value self)
{
    int rc = unqlite_commit(getcontext(mrb, self)->db);
    aux_check_error(mrb, rc, "unqlite_commit");

    return self;
}

static mrb_value
ext_abort(MRB, mrb_value self)
{
    int rc = unqlite_rollback(getcontext(mrb, self)->db);
    aux_check_error(mrb, rc, "unqlite_rollback");

    return self;
}

static struct RClass *
init_unqlite(MRB)
{
    struct RClass *cUnQLite = mrb_define_class(mrb, "UnQLite", mrb_cObject);
    MRB_SET_INSTANCE_TT(cUnQLite, MRB_TT_DATA);
    mrb_define_class_method(mrb, cUnQLite, "new", ext_s_new, MRB_ARGS_ANY());
    mrb_define_method(mrb, cUnQLite, "initialize", ext_initialize, MRB_ARGS_NONE());
    mrb_define_method(mrb, cUnQLite, "store", ext_store, MRB_ARGS_REQ(2));
    mrb_define_method(mrb, cUnQLite, "append", ext_append, MRB_ARGS_REQ(2));
    mrb_define_method(mrb, cUnQLite, "size", ext_size, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, cUnQLite, "fetch", ext_fetch, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, cUnQLite, "delete", ext_delete, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, cUnQLite, "close", ext_close, MRB_ARGS_NONE());
    mrb_define_method(mrb, cUnQLite, "transaction_begin", ext_begin, MRB_ARGS_NONE());
    mrb_define_method(mrb, cUnQLite, "transaction_commit", ext_commit, MRB_ARGS_NONE());
    mrb_define_method(mrb, cUnQLite, "transaction_abort", ext_abort, MRB_ARGS_NONE());

    return cUnQLite;
}

/*
 * class UnQLite::Cursor
 */

struct cursor
{
    unqlite *db;
    unqlite_kv_cursor *cursor;
};

static void
cur_free(MRB, struct cursor *p)
{
    if (p->cursor) {
        unqlite_kv_cursor_release(p->db, p->cursor);
        p->cursor = NULL;
    }

    mrb_free(mrb, p);
}

static const mrb_data_type cursor_type = {
    "cursor@mruby-unqlite",
    (void (*)(MRB, void *))cur_free,
};

static struct cursor *
get_cursorp(MRB, VALUE obj)
{
    return (struct cursor *)mrb_data_get_ptr(mrb, obj, &cursor_type);
}

static struct cursor *
get_cursor(MRB, VALUE obj)
{
    struct cursor *p = get_cursorp(mrb, obj);

    if (!p->db || !p->cursor) {
        mrb_raisef(mrb, E_TYPE_ERROR, "invalid reference - %S", obj);
    }

    return p;
}

static VALUE
cur_s_new(MRB, VALUE self)
{
    struct RData *data = mrb_data_object_alloc(mrb, mrb_class_ptr(self), NULL, &cursor_type);
    data->data = mrb_calloc(mrb, 1, sizeof(struct cursor));

    VALUE obj = VALUE(data);
    mrbx_funcall_passthrough(mrb, obj, id_initialize);

    return obj;
}

static VALUE
cur_initialize(MRB, VALUE self)
{
    VALUE db;
    mrb_get_args(mrb, "o", &db);

    struct context *dbp = getcontext(mrb, db);

    struct cursor *p = get_cursorp(mrb, self);
    if (p->db) {
        mrb_raisef(mrb, E_RUNTIME_ERROR, "invalid re-initialization - %S", self);
    }

    int rc = unqlite_kv_cursor_init(dbp->db, &p->cursor);
    aux_check_error(mrb, rc, "unqlite_kv_cursor_init");

    p->db = dbp->db;
    mrb_iv_set(mrb, self, SYMBOL("owner@mruby-unqlite"), db);

    rc = unqlite_kv_cursor_reset(p->cursor);
    aux_check_error(mrb, rc, "unqlite_kv_cursor_reset");

    return self;
}

static VALUE
cur_release(MRB, VALUE self)
{
    mrb_get_args(mrb, "");

    struct cursor *p = get_cursor(mrb, self);

    unqlite_kv_cursor_release(p->db, p->cursor);

    p->db = NULL;
    p->cursor = NULL;

    return Qnil;
}

static VALUE
cur_reset(MRB, VALUE self)
{
    mrb_get_args(mrb, "");

    struct cursor *p = get_cursor(mrb, self);

    int rc = unqlite_kv_cursor_reset(p->cursor);
    aux_check_error(mrb, rc, "unqlite_kv_cursor_reset");

    return self;
}

static VALUE
cur_seek(MRB, VALUE self)
{
    char *key;
    mrb_int keylen;
    VALUE pos;
    mrb_get_args(mrb, "s|o", &key, &keylen, &pos);

    struct cursor *p = get_cursor(mrb, self);

    int rc = unqlite_kv_cursor_seek(p->cursor, key, keylen, aux_convert_seek_pos(mrb, pos));
    if (rc == UNQLITE_DONE) { return Qnil; }
    aux_check_error(mrb, rc, "unqlite_kv_cursor_seek");

    return self;
}

static VALUE
cur_first(MRB, VALUE self)
{
    mrb_get_args(mrb, "");

    struct cursor *p = get_cursor(mrb, self);

    int rc = unqlite_kv_cursor_first_entry(p->cursor);
    if (rc == UNQLITE_DONE) { return Qnil; }
    aux_check_error(mrb, rc, "unqlite_kv_cursor_first_entry");

    return self;
}

static VALUE
cur_last(MRB, VALUE self)
{
    mrb_get_args(mrb, "");

    struct cursor *p = get_cursor(mrb, self);

    int rc = unqlite_kv_cursor_last_entry(p->cursor);
    if (rc == UNQLITE_DONE) { return Qnil; }
    aux_check_error(mrb, rc, "unqlite_kv_cursor_last_entry");

    return self;
}

static VALUE
cur_next(MRB, VALUE self)
{
    mrb_get_args(mrb, "");

    struct cursor *p = get_cursor(mrb, self);

    int rc = unqlite_kv_cursor_next_entry(p->cursor);
    if (rc == UNQLITE_DONE) { return Qnil; }
    aux_check_error(mrb, rc, "unqlite_kv_cursor_next_entry");

    return self;
}

static VALUE
cur_prev(MRB, VALUE self)
{
    mrb_get_args(mrb, "");

    struct cursor *p = get_cursor(mrb, self);

    int rc = unqlite_kv_cursor_prev_entry(p->cursor);
    if (rc == UNQLITE_DONE) { return Qnil; }
    aux_check_error(mrb, rc, "unqlite_kv_cursor_prev_entry");

    return self;
}

static VALUE
cur_valid(MRB, VALUE self)
{
    mrb_get_args(mrb, "");

    struct cursor *p = get_cursor(mrb, self);

    int rc = unqlite_kv_cursor_valid_entry(p->cursor);
    if (rc != 0 && rc != 1) {
        aux_check_error(mrb, rc, "unqlite_kv_cursor_valid_entry");
    }

    return (rc == 1 ? Qtrue : Qfalse);
}

static VALUE
cur_key(MRB, VALUE self)
{
    mrb_get_args(mrb, "");

    struct cursor *p = get_cursor(mrb, self);

    int keylen = 0;
    int rc = unqlite_kv_cursor_key(p->cursor, NULL, &keylen);
    if (rc == UNQLITE_INVALID) {
        /* 現在位置は何も指していない */
        return Qnil;
    }
    aux_check_error(mrb, rc, "unqlite_kv_cursor_key");

#if INT_MAX > MRBX_STR_MAX
    if (keylen > MRBX_STR_MAX) { keylen = MRBX_STR_MAX; }
#endif

    struct RString *key = RSTRING(mrb_str_buf_new(mrb, keylen));

    rc = unqlite_kv_cursor_key(p->cursor, RSTR_PTR(key), &keylen);
    aux_check_error(mrb, rc, "unqlite_kv_cursor_key");

    RSTR_SET_LEN(key, keylen);

    return VALUE(key);
}

static VALUE
cur_data(MRB, VALUE self)
{
    mrb_get_args(mrb, "");

    struct cursor *p = get_cursor(mrb, self);

    unqlite_int64 datalen = 0;
    int rc = unqlite_kv_cursor_data(p->cursor, NULL, &datalen);
    if (rc == UNQLITE_INVALID) {
        /* 現在位置は何も指していない */
        return Qnil;
    }
    aux_check_error(mrb, rc, "unqlite_kv_cursor_data");

#if INT64_MAX > MRBX_STR_MAX
    if (datalen > MRBX_STR_MAX) { datalen = MRBX_STR_MAX; }
#endif

    struct RString *data = RSTRING(mrb_str_buf_new(mrb, datalen));

    rc = unqlite_kv_cursor_data(p->cursor, RSTR_PTR(data), &datalen);
    aux_check_error(mrb, rc, "unqlite_kv_cursor_data");

    RSTR_SET_LEN(data, datalen);

    return VALUE(data);
}

static VALUE
cur_delete(MRB, VALUE self)
{
    mrb_get_args(mrb, "");

    struct cursor *p = get_cursor(mrb, self);

    int rc = unqlite_kv_cursor_delete_entry(p->cursor);
    aux_check_error(mrb, rc, "unqlite_kv_cursor_delete_entry");

    return self;
}

static struct RClass *
init_cursor(MRB, struct RClass *cUnQLite)
{
    struct RClass *cCursor = mrb_define_class_under(mrb, cUnQLite, "Cursor", mrb->object_class);
    MRB_SET_INSTANCE_TT(cCursor, MRB_TT_DATA);
    mrb_define_class_method(mrb, cCursor, "new", cur_s_new, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, cCursor, "initialize", cur_initialize, MRB_ARGS_REQ(1));
    mrb_define_method(mrb, cCursor, "release", cur_release, MRB_ARGS_NONE());
    mrb_define_method(mrb, cCursor, "reset", cur_reset, MRB_ARGS_NONE());
    mrb_define_method(mrb, cCursor, "seek", cur_seek, MRB_ARGS_REQ(2));
    mrb_define_method(mrb, cCursor, "first", cur_first, MRB_ARGS_NONE());
    mrb_define_method(mrb, cCursor, "last", cur_last, MRB_ARGS_NONE());
    mrb_define_method(mrb, cCursor, "next", cur_next, MRB_ARGS_NONE());
    mrb_define_method(mrb, cCursor, "prev", cur_prev, MRB_ARGS_NONE());
    mrb_define_method(mrb, cCursor, "valid?", cur_valid, MRB_ARGS_NONE());
    mrb_define_method(mrb, cCursor, "key", cur_key, MRB_ARGS_NONE());
    mrb_define_method(mrb, cCursor, "data", cur_data, MRB_ARGS_NONE());
    mrb_define_method(mrb, cCursor, "delete", cur_delete, MRB_ARGS_NONE());

    return cCursor;
}

/*
 * library mruby-unqlite
 */

void
mrb_mruby_unqlite_gem_init(MRB)
{
    struct RClass *cUnQLite = init_unqlite(mrb);
    struct RClass *cCursor = init_cursor(mrb, cUnQLite);
    (void)cCursor;
}

void
mrb_mruby_unqlite_gem_final(MRB)
{
}
