#ifndef CCB_CLI_H
#define CCB_CLI_H

#include "ccb_types.h"

/* Print top-level CLI help text. */
void usage(void);

/* Parse global flags and shift argc/argv to the subcommand segment. */
int parse_global_options(int *argc, char ***argv, GlobalOptions *out);

/* Convert command string (write/read/list/init-meta) to enum. */
int parse_command_type(const char *s, CommandType *type);

/* Parse subcommand long options into ParsedArgs. */
int parse_subcommand_args(int argc, char **argv, ParsedArgs *out);

/* Command-specific argument validators. Return 0 on success. */
int validate_write_args(const ParsedArgs *a);
int validate_read_args(const ParsedArgs *a);
int validate_list_args(const ParsedArgs *a);
int validate_init_meta_args(const ParsedArgs *a);
int validate_storage_write_args(const ParsedArgs *a);
int validate_network_send_args(const ParsedArgs *a);
int validate_ddr_pattern_store_args(const ParsedArgs *a);
int validate_ssd_lba_wrap_test_args(const ParsedArgs *a);
int validate_ssd_continuous_pattern_test_args(const ParsedArgs *a);

#endif
