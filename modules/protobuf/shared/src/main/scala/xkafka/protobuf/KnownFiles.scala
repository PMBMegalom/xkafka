/*
 * Copyright (c) 2026 xkafka contributors
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package xkafka.protobuf

/** The shapes of the other files Confluent's serializers know without references: Confluent's own types, Google's common types, and the rest of
  * Protobuf's well-known files, so a schema that imports one, as Kafka Connect's do for decimals and dates, is read. Each is the file as Confluent's
  * library gives it, read and written again as xkafka keeps it: its messages, enums and fields, without comments, options or extensions.
  */
private[protobuf] object KnownFiles:
  val texts: Map[String, String] =
    Map(
      "confluent/meta.proto" -> """syntax = "proto3";

package confluent;

import "google/protobuf/descriptor.proto";

message Meta {
  string doc = 1;
  map<string, string> params = 2;
  repeated string tags = 3;
  repeated .confluent.Rule rules = 4;
}

message Rule {
  string name = 1;
  string doc = 2;
  string expr = 3;
  string sql = 4;
}
""",
      "confluent/type/decimal.proto" -> """syntax = "proto3";

package confluent.type;

message Decimal {
  bytes value = 1;
  uint32 precision = 2;
  int32 scale = 3;
}
""",
      "google/protobuf/api.proto" -> """syntax = "proto3";

package google.protobuf;

import "google/protobuf/source_context.proto";
import "google/protobuf/type.proto";

message Api {
  string name = 1;
  repeated .google.protobuf.Method methods = 2;
  repeated .google.protobuf.Option options = 3;
  string version = 4;
  .google.protobuf.SourceContext source_context = 5;
  repeated .google.protobuf.Mixin mixins = 6;
  .google.protobuf.Syntax syntax = 7;
  string edition = 8;
}

message Method {
  string name = 1;
  string request_type_url = 2;
  bool request_streaming = 3;
  string response_type_url = 4;
  bool response_streaming = 5;
  repeated .google.protobuf.Option options = 6;
  .google.protobuf.Syntax syntax = 7;
  string edition = 8;
}

message Mixin {
  string name = 1;
  string root = 2;
}
""",
      "google/protobuf/descriptor.proto" -> """syntax = "proto2";

package google.protobuf;

message FileDescriptorSet {
  repeated .google.protobuf.FileDescriptorProto file = 1;
}

message FileDescriptorProto {
  optional string name = 1;
  optional string package = 2;
  repeated string dependency = 3;
  repeated int32 public_dependency = 10;
  repeated int32 weak_dependency = 11;
  repeated string option_dependency = 15;
  repeated .google.protobuf.DescriptorProto message_type = 4;
  repeated .google.protobuf.EnumDescriptorProto enum_type = 5;
  repeated .google.protobuf.ServiceDescriptorProto service = 6;
  repeated .google.protobuf.FieldDescriptorProto extension = 7;
  optional .google.protobuf.FileOptions options = 8;
  optional .google.protobuf.SourceCodeInfo source_code_info = 9;
  optional string syntax = 12;
  optional .google.protobuf.Edition edition = 14;
}

message DescriptorProto {
  optional string name = 1;
  repeated .google.protobuf.FieldDescriptorProto field = 2;
  repeated .google.protobuf.FieldDescriptorProto extension = 6;
  repeated .google.protobuf.DescriptorProto nested_type = 3;
  repeated .google.protobuf.EnumDescriptorProto enum_type = 4;
  repeated .google.protobuf.DescriptorProto.ExtensionRange extension_range = 5;
  repeated .google.protobuf.OneofDescriptorProto oneof_decl = 8;
  optional .google.protobuf.MessageOptions options = 7;
  repeated .google.protobuf.DescriptorProto.ReservedRange reserved_range = 9;
  repeated string reserved_name = 10;
  optional .google.protobuf.SymbolVisibility visibility = 11;
  message ExtensionRange {
    optional int32 start = 1;
    optional int32 end = 2;
    optional .google.protobuf.ExtensionRangeOptions options = 3;
  }
  message ReservedRange {
    optional int32 start = 1;
    optional int32 end = 2;
  }
}

message ExtensionRangeOptions {
  repeated .google.protobuf.UninterpretedOption uninterpreted_option = 999;
  repeated .google.protobuf.ExtensionRangeOptions.Declaration declaration = 2;
  optional .google.protobuf.FeatureSet features = 50;
  optional .google.protobuf.ExtensionRangeOptions.VerificationState verification = 3 [default = UNVERIFIED];
  message Declaration {
    optional int32 number = 1;
    optional string full_name = 2;
    optional string type = 3;
    optional bool reserved = 5;
    optional bool repeated = 6;
  }
  enum VerificationState {
    DECLARATION = 0;
    UNVERIFIED = 1;
  }
}

message FieldDescriptorProto {
  optional string name = 1;
  optional int32 number = 3;
  optional .google.protobuf.FieldDescriptorProto.Label label = 4;
  optional .google.protobuf.FieldDescriptorProto.Type type = 5;
  optional string type_name = 6;
  optional string extendee = 2;
  optional string default_value = 7;
  optional int32 oneof_index = 9;
  optional string json_name = 10;
  optional .google.protobuf.FieldOptions options = 8;
  optional bool proto3_optional = 17;
  enum Type {
    TYPE_DOUBLE = 1;
    TYPE_FLOAT = 2;
    TYPE_INT64 = 3;
    TYPE_UINT64 = 4;
    TYPE_INT32 = 5;
    TYPE_FIXED64 = 6;
    TYPE_FIXED32 = 7;
    TYPE_BOOL = 8;
    TYPE_STRING = 9;
    TYPE_GROUP = 10;
    TYPE_MESSAGE = 11;
    TYPE_BYTES = 12;
    TYPE_UINT32 = 13;
    TYPE_ENUM = 14;
    TYPE_SFIXED32 = 15;
    TYPE_SFIXED64 = 16;
    TYPE_SINT32 = 17;
    TYPE_SINT64 = 18;
  }
  enum Label {
    LABEL_OPTIONAL = 1;
    LABEL_REPEATED = 3;
    LABEL_REQUIRED = 2;
  }
}

message OneofDescriptorProto {
  optional string name = 1;
  optional .google.protobuf.OneofOptions options = 2;
}

message EnumDescriptorProto {
  optional string name = 1;
  repeated .google.protobuf.EnumValueDescriptorProto value = 2;
  optional .google.protobuf.EnumOptions options = 3;
  repeated .google.protobuf.EnumDescriptorProto.EnumReservedRange reserved_range = 4;
  repeated string reserved_name = 5;
  optional .google.protobuf.SymbolVisibility visibility = 6;
  message EnumReservedRange {
    optional int32 start = 1;
    optional int32 end = 2;
  }
}

message EnumValueDescriptorProto {
  optional string name = 1;
  optional int32 number = 2;
  optional .google.protobuf.EnumValueOptions options = 3;
}

message ServiceDescriptorProto {
  optional string name = 1;
  repeated .google.protobuf.MethodDescriptorProto method = 2;
  optional .google.protobuf.ServiceOptions options = 3;
}

message MethodDescriptorProto {
  optional string name = 1;
  optional string input_type = 2;
  optional string output_type = 3;
  optional .google.protobuf.MethodOptions options = 4;
  optional bool client_streaming = 5 [default = false];
  optional bool server_streaming = 6 [default = false];
}

message FileOptions {
  optional string java_package = 1;
  optional string java_outer_classname = 8;
  optional bool java_multiple_files = 10 [default = false];
  optional bool java_generate_equals_and_hash = 20;
  optional bool java_string_check_utf8 = 27 [default = false];
  optional .google.protobuf.FileOptions.OptimizeMode optimize_for = 9 [default = SPEED];
  optional string go_package = 11;
  optional bool cc_generic_services = 16 [default = false];
  optional bool java_generic_services = 17 [default = false];
  optional bool py_generic_services = 18 [default = false];
  optional bool deprecated = 23 [default = false];
  optional bool cc_enable_arenas = 31 [default = true];
  optional string objc_class_prefix = 36;
  optional string csharp_namespace = 37;
  optional string swift_prefix = 39;
  optional string php_class_prefix = 40;
  optional string php_namespace = 41;
  optional string php_metadata_namespace = 44;
  optional string ruby_package = 45;
  optional .google.protobuf.FeatureSet features = 50;
  repeated .google.protobuf.UninterpretedOption uninterpreted_option = 999;
  enum OptimizeMode {
    SPEED = 1;
    CODE_SIZE = 2;
    LITE_RUNTIME = 3;
  }
}

message MessageOptions {
  optional bool message_set_wire_format = 1 [default = false];
  optional bool no_standard_descriptor_accessor = 2 [default = false];
  optional bool deprecated = 3 [default = false];
  optional bool map_entry = 7;
  optional bool deprecated_legacy_json_field_conflicts = 11;
  optional .google.protobuf.FeatureSet features = 12;
  repeated .google.protobuf.UninterpretedOption uninterpreted_option = 999;
}

message FieldOptions {
  optional .google.protobuf.FieldOptions.CType ctype = 1 [default = STRING];
  optional bool packed = 2;
  optional .google.protobuf.FieldOptions.JSType jstype = 6 [default = JS_NORMAL];
  optional bool lazy = 5 [default = false];
  optional bool unverified_lazy = 15 [default = false];
  optional bool deprecated = 3 [default = false];
  optional bool weak = 10 [default = false];
  optional bool debug_redact = 16 [default = false];
  optional .google.protobuf.FieldOptions.OptionRetention retention = 17;
  repeated .google.protobuf.FieldOptions.OptionTargetType targets = 19;
  repeated .google.protobuf.FieldOptions.EditionDefault edition_defaults = 20;
  optional .google.protobuf.FeatureSet features = 21;
  optional .google.protobuf.FieldOptions.FeatureSupport feature_support = 22;
  repeated .google.protobuf.UninterpretedOption uninterpreted_option = 999;
  message EditionDefault {
    optional .google.protobuf.Edition edition = 3;
    optional string value = 2;
  }
  message FeatureSupport {
    optional .google.protobuf.Edition edition_introduced = 1;
    optional .google.protobuf.Edition edition_deprecated = 2;
    optional string deprecation_warning = 3;
    optional .google.protobuf.Edition edition_removed = 4;
    optional string removal_error = 5;
  }
  enum CType {
    STRING = 0;
    CORD = 1;
    STRING_PIECE = 2;
  }
  enum JSType {
    JS_NORMAL = 0;
    JS_STRING = 1;
    JS_NUMBER = 2;
  }
  enum OptionRetention {
    RETENTION_UNKNOWN = 0;
    RETENTION_RUNTIME = 1;
    RETENTION_SOURCE = 2;
  }
  enum OptionTargetType {
    TARGET_TYPE_UNKNOWN = 0;
    TARGET_TYPE_FILE = 1;
    TARGET_TYPE_EXTENSION_RANGE = 2;
    TARGET_TYPE_MESSAGE = 3;
    TARGET_TYPE_FIELD = 4;
    TARGET_TYPE_ONEOF = 5;
    TARGET_TYPE_ENUM = 6;
    TARGET_TYPE_ENUM_ENTRY = 7;
    TARGET_TYPE_SERVICE = 8;
    TARGET_TYPE_METHOD = 9;
  }
}

message OneofOptions {
  optional .google.protobuf.FeatureSet features = 1;
  repeated .google.protobuf.UninterpretedOption uninterpreted_option = 999;
}

message EnumOptions {
  optional bool allow_alias = 2;
  optional bool deprecated = 3 [default = false];
  optional bool deprecated_legacy_json_field_conflicts = 6;
  optional .google.protobuf.FeatureSet features = 7;
  repeated .google.protobuf.UninterpretedOption uninterpreted_option = 999;
}

message EnumValueOptions {
  optional bool deprecated = 1 [default = false];
  optional .google.protobuf.FeatureSet features = 2;
  optional bool debug_redact = 3 [default = false];
  optional .google.protobuf.FieldOptions.FeatureSupport feature_support = 4;
  repeated .google.protobuf.UninterpretedOption uninterpreted_option = 999;
}

message ServiceOptions {
  optional .google.protobuf.FeatureSet features = 34;
  optional bool deprecated = 33 [default = false];
  repeated .google.protobuf.UninterpretedOption uninterpreted_option = 999;
}

message MethodOptions {
  optional bool deprecated = 33 [default = false];
  optional .google.protobuf.MethodOptions.IdempotencyLevel idempotency_level = 34 [default = IDEMPOTENCY_UNKNOWN];
  optional .google.protobuf.FeatureSet features = 35;
  repeated .google.protobuf.UninterpretedOption uninterpreted_option = 999;
  enum IdempotencyLevel {
    IDEMPOTENCY_UNKNOWN = 0;
    NO_SIDE_EFFECTS = 1;
    IDEMPOTENT = 2;
  }
}

message UninterpretedOption {
  repeated .google.protobuf.UninterpretedOption.NamePart name = 2;
  optional string identifier_value = 3;
  optional uint64 positive_int_value = 4;
  optional int64 negative_int_value = 5;
  optional double double_value = 6;
  optional bytes string_value = 7;
  optional string aggregate_value = 8;
  message NamePart {
    required string name_part = 1;
    required bool is_extension = 2;
  }
}

message FeatureSet {
  optional .google.protobuf.FeatureSet.FieldPresence field_presence = 1;
  optional .google.protobuf.FeatureSet.EnumType enum_type = 2;
  optional .google.protobuf.FeatureSet.RepeatedFieldEncoding repeated_field_encoding = 3;
  optional .google.protobuf.FeatureSet.Utf8Validation utf8_validation = 4;
  optional .google.protobuf.FeatureSet.MessageEncoding message_encoding = 5;
  optional .google.protobuf.FeatureSet.JsonFormat json_format = 6;
  optional .google.protobuf.FeatureSet.EnforceNamingStyle enforce_naming_style = 7;
  optional .google.protobuf.FeatureSet.VisibilityFeature.DefaultSymbolVisibility default_symbol_visibility = 8;
  message VisibilityFeature {
    enum DefaultSymbolVisibility {
      DEFAULT_SYMBOL_VISIBILITY_UNKNOWN = 0;
      EXPORT_ALL = 1;
      EXPORT_TOP_LEVEL = 2;
      LOCAL_ALL = 3;
      STRICT = 4;
    }
  }
  enum FieldPresence {
    FIELD_PRESENCE_UNKNOWN = 0;
    EXPLICIT = 1;
    IMPLICIT = 2;
    LEGACY_REQUIRED = 3;
  }
  enum EnumType {
    ENUM_TYPE_UNKNOWN = 0;
    OPEN = 1;
    CLOSED = 2;
  }
  enum RepeatedFieldEncoding {
    REPEATED_FIELD_ENCODING_UNKNOWN = 0;
    PACKED = 1;
    EXPANDED = 2;
  }
  enum Utf8Validation {
    UTF8_VALIDATION_UNKNOWN = 0;
    VERIFY = 2;
    NONE = 3;
  }
  enum MessageEncoding {
    MESSAGE_ENCODING_UNKNOWN = 0;
    LENGTH_PREFIXED = 1;
    DELIMITED = 2;
  }
  enum JsonFormat {
    JSON_FORMAT_UNKNOWN = 0;
    ALLOW = 1;
    LEGACY_BEST_EFFORT = 2;
  }
  enum EnforceNamingStyle {
    ENFORCE_NAMING_STYLE_UNKNOWN = 0;
    STYLE2024 = 1;
    STYLE_LEGACY = 2;
  }
}

message FeatureSetDefaults {
  repeated .google.protobuf.FeatureSetDefaults.FeatureSetEditionDefault defaults = 1;
  optional .google.protobuf.Edition minimum_edition = 4;
  optional .google.protobuf.Edition maximum_edition = 5;
  message FeatureSetEditionDefault {
    optional .google.protobuf.Edition edition = 3;
    optional .google.protobuf.FeatureSet overridable_features = 4;
    optional .google.protobuf.FeatureSet fixed_features = 5;
  }
}

message SourceCodeInfo {
  repeated .google.protobuf.SourceCodeInfo.Location location = 1;
  message Location {
    repeated int32 path = 1 [packed = true];
    repeated int32 span = 2 [packed = true];
    optional string leading_comments = 3;
    optional string trailing_comments = 4;
    repeated string leading_detached_comments = 6;
  }
}

message GeneratedCodeInfo {
  repeated .google.protobuf.GeneratedCodeInfo.Annotation annotation = 1;
  message Annotation {
    repeated int32 path = 1 [packed = true];
    optional string source_file = 2;
    optional int32 begin = 3;
    optional int32 end = 4;
    optional .google.protobuf.GeneratedCodeInfo.Annotation.Semantic semantic = 5;
    enum Semantic {
      NONE = 0;
      SET = 1;
      ALIAS = 2;
    }
  }
}

enum Edition {
  EDITION_UNKNOWN = 0;
  EDITION_LEGACY = 900;
  EDITION_PROTO2 = 998;
  EDITION_PROTO3 = 999;
  EDITION_2023 = 1000;
  EDITION_2024 = 1001;
  EDITION_UNSTABLE = 9999;
  EDITION_1_TEST_ONLY = 1;
  EDITION_2_TEST_ONLY = 2;
  EDITION_99997_TEST_ONLY = 99997;
  EDITION_99998_TEST_ONLY = 99998;
  EDITION_99999_TEST_ONLY = 99999;
  EDITION_MAX = 2147483647;
}

enum SymbolVisibility {
  VISIBILITY_UNSET = 0;
  VISIBILITY_LOCAL = 1;
  VISIBILITY_EXPORT = 2;
}
""",
      "google/protobuf/source_context.proto" -> """syntax = "proto3";

package google.protobuf;

message SourceContext {
  string file_name = 1;
}
""",
      "google/protobuf/type.proto" -> """syntax = "proto3";

package google.protobuf;

import "google/protobuf/any.proto";
import "google/protobuf/source_context.proto";

message Type {
  string name = 1;
  repeated .google.protobuf.Field fields = 2;
  repeated string oneofs = 3;
  repeated .google.protobuf.Option options = 4;
  .google.protobuf.SourceContext source_context = 5;
  .google.protobuf.Syntax syntax = 6;
  string edition = 7;
}

message Field {
  .google.protobuf.Field.Kind kind = 1;
  .google.protobuf.Field.Cardinality cardinality = 2;
  int32 number = 3;
  string name = 4;
  string type_url = 6;
  int32 oneof_index = 7;
  bool packed = 8;
  repeated .google.protobuf.Option options = 9;
  string json_name = 10;
  string default_value = 11;
  enum Kind {
    TYPE_UNKNOWN = 0;
    TYPE_DOUBLE = 1;
    TYPE_FLOAT = 2;
    TYPE_INT64 = 3;
    TYPE_UINT64 = 4;
    TYPE_INT32 = 5;
    TYPE_FIXED64 = 6;
    TYPE_FIXED32 = 7;
    TYPE_BOOL = 8;
    TYPE_STRING = 9;
    TYPE_GROUP = 10;
    TYPE_MESSAGE = 11;
    TYPE_BYTES = 12;
    TYPE_UINT32 = 13;
    TYPE_ENUM = 14;
    TYPE_SFIXED32 = 15;
    TYPE_SFIXED64 = 16;
    TYPE_SINT32 = 17;
    TYPE_SINT64 = 18;
  }
  enum Cardinality {
    CARDINALITY_UNKNOWN = 0;
    CARDINALITY_OPTIONAL = 1;
    CARDINALITY_REQUIRED = 2;
    CARDINALITY_REPEATED = 3;
  }
}

message Enum {
  string name = 1;
  repeated .google.protobuf.EnumValue enumvalue = 2;
  repeated .google.protobuf.Option options = 3;
  .google.protobuf.SourceContext source_context = 4;
  .google.protobuf.Syntax syntax = 5;
  string edition = 6;
}

message EnumValue {
  string name = 1;
  int32 number = 2;
  repeated .google.protobuf.Option options = 3;
}

message Option {
  string name = 1;
  .google.protobuf.Any value = 2;
}

enum Syntax {
  SYNTAX_PROTO2 = 0;
  SYNTAX_PROTO3 = 1;
  SYNTAX_EDITIONS = 2;
}
""",
      "google/type/calendar_period.proto" -> """syntax = "proto3";

package google.type;

enum CalendarPeriod {
  CALENDAR_PERIOD_UNSPECIFIED = 0;
  DAY = 1;
  WEEK = 2;
  FORTNIGHT = 3;
  MONTH = 4;
  QUARTER = 5;
  HALF = 6;
  YEAR = 7;
}
""",
      "google/type/color.proto" -> """syntax = "proto3";

package google.type;

import "google/protobuf/wrappers.proto";

message Color {
  float red = 1;
  float green = 2;
  float blue = 3;
  .google.protobuf.FloatValue alpha = 4;
}
""",
      "google/type/date.proto" -> """syntax = "proto3";

package google.type;

message Date {
  int32 year = 1;
  int32 month = 2;
  int32 day = 3;
}
""",
      "google/type/datetime.proto" -> """syntax = "proto3";

package google.type;

import "google/protobuf/duration.proto";

message DateTime {
  int32 year = 1;
  int32 month = 2;
  int32 day = 3;
  int32 hours = 4;
  int32 minutes = 5;
  int32 seconds = 6;
  int32 nanos = 7;
  oneof time_offset {
    .google.protobuf.Duration utc_offset = 8;
    .google.type.TimeZone time_zone = 9;
  }
}

message TimeZone {
  string id = 1;
  string version = 2;
}
""",
      "google/type/dayofweek.proto" -> """syntax = "proto3";

package google.type;

enum DayOfWeek {
  DAY_OF_WEEK_UNSPECIFIED = 0;
  MONDAY = 1;
  TUESDAY = 2;
  WEDNESDAY = 3;
  THURSDAY = 4;
  FRIDAY = 5;
  SATURDAY = 6;
  SUNDAY = 7;
}
""",
      "google/type/decimal.proto" -> """syntax = "proto3";

package google.type;

message Decimal {
  string value = 1;
}
""",
      "google/type/expr.proto" -> """syntax = "proto3";

package google.type;

message Expr {
  string expression = 1;
  string title = 2;
  string description = 3;
  string location = 4;
}
""",
      "google/type/fraction.proto" -> """syntax = "proto3";

package google.type;

message Fraction {
  int64 numerator = 1;
  int64 denominator = 2;
}
""",
      "google/type/interval.proto" -> """syntax = "proto3";

package google.type;

import "google/protobuf/timestamp.proto";

message Interval {
  .google.protobuf.Timestamp start_time = 1;
  .google.protobuf.Timestamp end_time = 2;
}
""",
      "google/type/latlng.proto" -> """syntax = "proto3";

package google.type;

message LatLng {
  double latitude = 1;
  double longitude = 2;
}
""",
      "google/type/money.proto" -> """syntax = "proto3";

package google.type;

message Money {
  string currency_code = 1;
  int64 units = 2;
  int32 nanos = 3;
}
""",
      "google/type/month.proto" -> """syntax = "proto3";

package google.type;

enum Month {
  MONTH_UNSPECIFIED = 0;
  JANUARY = 1;
  FEBRUARY = 2;
  MARCH = 3;
  APRIL = 4;
  MAY = 5;
  JUNE = 6;
  JULY = 7;
  AUGUST = 8;
  SEPTEMBER = 9;
  OCTOBER = 10;
  NOVEMBER = 11;
  DECEMBER = 12;
}
""",
      "google/type/phone_number.proto" -> """syntax = "proto3";

package google.type;

message PhoneNumber {
  string extension = 3;
  oneof kind {
    string e164_number = 1;
    .google.type.PhoneNumber.ShortCode short_code = 2;
  }
  message ShortCode {
    string region_code = 1;
    string number = 2;
  }
}
""",
      "google/type/postal_address.proto" -> """syntax = "proto3";

package google.type;

message PostalAddress {
  int32 revision = 1;
  string region_code = 2;
  string language_code = 3;
  string postal_code = 4;
  string sorting_code = 5;
  string administrative_area = 6;
  string locality = 7;
  string sublocality = 8;
  repeated string address_lines = 9;
  repeated string recipients = 10;
  string organization = 11;
}
""",
      "google/type/quaternion.proto" -> """syntax = "proto3";

package google.type;

message Quaternion {
  double x = 1;
  double y = 2;
  double z = 3;
  double w = 4;
}
""",
      "google/type/timeofday.proto" -> """syntax = "proto3";

package google.type;

message TimeOfDay {
  int32 hours = 1;
  int32 minutes = 2;
  int32 seconds = 3;
  int32 nanos = 4;
}
"""
    )
