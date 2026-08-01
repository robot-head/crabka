// PostgreSQL 18 PL/pgSQL error conditions generated from:
// https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/utils/errcodes.txt
//
// Like PostgreSQL's generate-plerrcodes.pl, this intentionally includes only
// error entries that have a PL/pgSQL condition name. Some names map to more
// than one SQLSTATE, so callers must consider every resolved code.
const CONDITIONS: &str = "\
03000 sql_statement_not_yet_complete
08000 connection_exception
08003 connection_does_not_exist
08006 connection_failure
08001 sqlclient_unable_to_establish_sqlconnection
08004 sqlserver_rejected_establishment_of_sqlconnection
08007 transaction_resolution_unknown
08P01 protocol_violation
09000 triggered_action_exception
0A000 feature_not_supported
0B000 invalid_transaction_initiation
0F000 locator_exception
0F001 invalid_locator_specification
0L000 invalid_grantor
0LP01 invalid_grant_operation
0P000 invalid_role_specification
0Z000 diagnostics_exception
0Z002 stacked_diagnostics_accessed_without_active_handler
10608 invalid_argument_for_xquery
20000 case_not_found
21000 cardinality_violation
22000 data_exception
2202E array_subscript_error
22021 character_not_in_repertoire
22008 datetime_field_overflow
22012 division_by_zero
22005 error_in_assignment
2200B escape_character_conflict
22022 indicator_overflow
22015 interval_field_overflow
2201E invalid_argument_for_logarithm
22014 invalid_argument_for_ntile_function
22016 invalid_argument_for_nth_value_function
2201F invalid_argument_for_power_function
2201G invalid_argument_for_width_bucket_function
22018 invalid_character_value_for_cast
22007 invalid_datetime_format
22019 invalid_escape_character
2200D invalid_escape_octet
22025 invalid_escape_sequence
22P06 nonstandard_use_of_escape_character
22010 invalid_indicator_parameter_value
22023 invalid_parameter_value
22013 invalid_preceding_or_following_size
2201B invalid_regular_expression
2201W invalid_row_count_in_limit_clause
2201X invalid_row_count_in_result_offset_clause
2202H invalid_tablesample_argument
2202G invalid_tablesample_repeat
22009 invalid_time_zone_displacement_value
2200C invalid_use_of_escape_character
2200G most_specific_type_mismatch
22004 null_value_not_allowed
22002 null_value_no_indicator_parameter
22003 numeric_value_out_of_range
2200H sequence_generator_limit_exceeded
22026 string_data_length_mismatch
22001 string_data_right_truncation
22011 substring_error
22027 trim_error
22024 unterminated_c_string
2200F zero_length_character_string
22P01 floating_point_exception
22P02 invalid_text_representation
22P03 invalid_binary_representation
22P04 bad_copy_file_format
22P05 untranslatable_character
2200L not_an_xml_document
2200M invalid_xml_document
2200N invalid_xml_content
2200S invalid_xml_comment
2200T invalid_xml_processing_instruction
22030 duplicate_json_object_key_value
22031 invalid_argument_for_sql_json_datetime_function
22032 invalid_json_text
22033 invalid_sql_json_subscript
22034 more_than_one_sql_json_item
22035 no_sql_json_item
22036 non_numeric_sql_json_item
22037 non_unique_keys_in_a_json_object
22038 singleton_sql_json_item_required
22039 sql_json_array_not_found
2203A sql_json_member_not_found
2203B sql_json_number_not_found
2203C sql_json_object_not_found
2203D too_many_json_array_elements
2203E too_many_json_object_members
2203F sql_json_scalar_required
2203G sql_json_item_cannot_be_cast_to_target_type
23000 integrity_constraint_violation
23001 restrict_violation
23502 not_null_violation
23503 foreign_key_violation
23505 unique_violation
23514 check_violation
23P01 exclusion_violation
24000 invalid_cursor_state
25000 invalid_transaction_state
25001 active_sql_transaction
25002 branch_transaction_already_active
25008 held_cursor_requires_same_isolation_level
25003 inappropriate_access_mode_for_branch_transaction
25004 inappropriate_isolation_level_for_branch_transaction
25005 no_active_sql_transaction_for_branch_transaction
25006 read_only_sql_transaction
25007 schema_and_data_statement_mixing_not_supported
25P01 no_active_sql_transaction
25P02 in_failed_sql_transaction
25P03 idle_in_transaction_session_timeout
25P04 transaction_timeout
26000 invalid_sql_statement_name
27000 triggered_data_change_violation
28000 invalid_authorization_specification
28P01 invalid_password
2B000 dependent_privilege_descriptors_still_exist
2BP01 dependent_objects_still_exist
2D000 invalid_transaction_termination
2F000 sql_routine_exception
2F005 function_executed_no_return_statement
2F002 modifying_sql_data_not_permitted
2F003 prohibited_sql_statement_attempted
2F004 reading_sql_data_not_permitted
34000 invalid_cursor_name
38000 external_routine_exception
38001 containing_sql_not_permitted
38002 modifying_sql_data_not_permitted
38003 prohibited_sql_statement_attempted
38004 reading_sql_data_not_permitted
39000 external_routine_invocation_exception
39001 invalid_sqlstate_returned
39004 null_value_not_allowed
39P01 trigger_protocol_violated
39P02 srf_protocol_violated
39P03 event_trigger_protocol_violated
3B000 savepoint_exception
3B001 invalid_savepoint_specification
3D000 invalid_catalog_name
3F000 invalid_schema_name
40000 transaction_rollback
40002 transaction_integrity_constraint_violation
40001 serialization_failure
40003 statement_completion_unknown
40P01 deadlock_detected
42000 syntax_error_or_access_rule_violation
42601 syntax_error
42501 insufficient_privilege
42846 cannot_coerce
42803 grouping_error
42P20 windowing_error
42P19 invalid_recursion
42830 invalid_foreign_key
42602 invalid_name
42622 name_too_long
42939 reserved_name
42804 datatype_mismatch
42P18 indeterminate_datatype
42P21 collation_mismatch
42P22 indeterminate_collation
42809 wrong_object_type
428C9 generated_always
42703 undefined_column
42883 undefined_function
42P01 undefined_table
42P02 undefined_parameter
42704 undefined_object
42701 duplicate_column
42P03 duplicate_cursor
42P04 duplicate_database
42723 duplicate_function
42P05 duplicate_prepared_statement
42P06 duplicate_schema
42P07 duplicate_table
42712 duplicate_alias
42710 duplicate_object
42702 ambiguous_column
42725 ambiguous_function
42P08 ambiguous_parameter
42P09 ambiguous_alias
42P10 invalid_column_reference
42611 invalid_column_definition
42P11 invalid_cursor_definition
42P12 invalid_database_definition
42P13 invalid_function_definition
42P14 invalid_prepared_statement_definition
42P15 invalid_schema_definition
42P16 invalid_table_definition
42P17 invalid_object_definition
44000 with_check_option_violation
53000 insufficient_resources
53100 disk_full
53200 out_of_memory
53300 too_many_connections
53400 configuration_limit_exceeded
54000 program_limit_exceeded
54001 statement_too_complex
54011 too_many_columns
54023 too_many_arguments
55000 object_not_in_prerequisite_state
55006 object_in_use
55P02 cant_change_runtime_param
55P03 lock_not_available
55P04 unsafe_new_enum_value_usage
57000 operator_intervention
57014 query_canceled
57P01 admin_shutdown
57P02 crash_shutdown
57P03 cannot_connect_now
57P04 database_dropped
57P05 idle_session_timeout
58000 system_error
58030 io_error
58P01 undefined_file
58P02 duplicate_file
58P03 file_name_too_long
F0000 config_file_error
F0001 lock_file_exists
HV000 fdw_error
HV005 fdw_column_name_not_found
HV002 fdw_dynamic_parameter_value_needed
HV010 fdw_function_sequence_error
HV021 fdw_inconsistent_descriptor_information
HV024 fdw_invalid_attribute_value
HV007 fdw_invalid_column_name
HV008 fdw_invalid_column_number
HV004 fdw_invalid_data_type
HV006 fdw_invalid_data_type_descriptors
HV091 fdw_invalid_descriptor_field_identifier
HV00B fdw_invalid_handle
HV00C fdw_invalid_option_index
HV00D fdw_invalid_option_name
HV090 fdw_invalid_string_length_or_buffer_length
HV00A fdw_invalid_string_format
HV009 fdw_invalid_use_of_null_pointer
HV014 fdw_too_many_handles
HV001 fdw_out_of_memory
HV00P fdw_no_schemas
HV00J fdw_option_name_not_found
HV00K fdw_reply_handle
HV00Q fdw_schema_not_found
HV00R fdw_table_not_found
HV00L fdw_unable_to_create_execution
HV00M fdw_unable_to_create_reply
HV00N fdw_unable_to_establish_connection
P0000 plpgsql_error
P0001 raise_exception
P0002 no_data_found
P0003 too_many_rows
P0004 assert_failure
XX000 internal_error
XX001 data_corrupted
XX002 index_corrupted
";

const QUERY_CANCELED: &str = "57014";
const ASSERT_FAILURE: &str = "P0004";

/// Resolve a PL/pgSQL condition name to every corresponding SQLSTATE.
pub(crate) fn resolve_condition(name: &str) -> impl Iterator<Item = &'static str> + '_ {
    CONDITIONS.lines().filter_map(move |line| {
        let (code, label) = line.split_once(' ')?;
        label.eq_ignore_ascii_case(name).then_some(code)
    })
}

/// Whether `code` is a SQLSTATE literal accepted by PL/pgSQL.
pub(crate) fn is_valid_sqlstate(code: &str) -> bool {
    code.len() == 5
        && code
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte.is_ascii_uppercase())
}

/// Match a named condition, literal SQLSTATE, or `OTHERS` against an error.
pub(crate) fn condition_matches(condition: &str, sqlstate: &str) -> bool {
    if !is_valid_sqlstate(sqlstate) {
        return false;
    }
    if condition.eq_ignore_ascii_case("others") {
        return sqlstate != QUERY_CANCELED && sqlstate != ASSERT_FAILURE;
    }
    if is_valid_sqlstate(condition) {
        return sqlstate_matches(condition, sqlstate);
    }
    resolve_condition(condition).any(|expected| sqlstate_matches(expected, sqlstate))
}

fn sqlstate_matches(expected: &str, actual: &str) -> bool {
    expected == actual || (expected.ends_with("000") && expected[..2] == actual[..2])
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn validates_sqlstate_literals() {
        for code in ["00000", "22012", "22P02", "P0004", "XX000"] {
            assert!(is_valid_sqlstate(code), "{code}");
        }
        for code in ["", "2201", "220123", "22p02", "22-02", "é2000"] {
            assert!(!is_valid_sqlstate(code), "{code}");
        }
    }

    #[test]
    fn resolves_all_named_conditions_including_duplicates() {
        assert!(CONDITIONS.lines().count() == 251);
        assert!(resolve_condition("unique_violation").collect::<Vec<_>>() == ["23505"]);
        assert!(resolve_condition("UNIQUE_VIOLATION").collect::<Vec<_>>() == ["23505"]);
        assert!(
            resolve_condition("modifying_sql_data_not_permitted").collect::<Vec<_>>()
                == ["2F002", "38002"]
        );
        assert!(
            resolve_condition("null_value_not_allowed").collect::<Vec<_>>() == ["22004", "39004"]
        );
        assert!(resolve_condition("warning").next().is_none());
        assert!(resolve_condition("successful_completion").next().is_none());
        assert!(resolve_condition("not_a_condition").next().is_none());
    }

    #[test]
    fn matches_exact_and_category_conditions() {
        let cases = [
            ("division_by_zero", "22012", true),
            ("division_by_zero", "22003", false),
            ("data_exception", "22012", true),
            ("data_exception", "22P02", true),
            ("22000", "22012", true),
            ("22012", "22012", true),
            ("22012", "22003", false),
            ("sql_routine_exception", "2F003", true),
            ("modifying_sql_data_not_permitted", "38002", true),
            ("not_a_condition", "22012", false),
        ];
        for (condition, sqlstate, expected) in cases {
            assert!(
                condition_matches(condition, sqlstate) == expected,
                "{condition} {sqlstate}"
            );
        }
    }

    #[test]
    fn others_excludes_cancel_and_assertion_failures() {
        assert!(condition_matches("others", "23505"));
        assert!(!condition_matches("others", QUERY_CANCELED));
        assert!(!condition_matches("others", ASSERT_FAILURE));
        assert!(condition_matches("query_canceled", QUERY_CANCELED));
        assert!(condition_matches("assert_failure", ASSERT_FAILURE));
    }
}
