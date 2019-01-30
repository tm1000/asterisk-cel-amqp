/*
 * Asterisk -- An open source telephony toolkit.
 *
 * Copyright (C) 2015, Digium, Inc.
 *
 * David M. Lee, II <dlee@digium.com>
 *
 * See http://www.asterisk.org for more information about
 * the Asterisk project. Please do not directly contact
 * any of the maintainers of this project for assistance;
 * the project provides a web site, mailing lists and IRC
 * channels for your use.
 *
 * This program is free software, distributed under the terms of
 * the GNU General Public License Version 2. See the LICENSE file
 * at the top of the source tree.
 */

/*! \file
 *
 * \brief AMQP CEL Backend
 *
 * \author David M. Lee, II <dlee@digium.com>
 */

/*** MODULEINFO
	<depend>res_amqp</depend>
	<support_level>core</support_level>
 ***/

/*** DOCUMENTATION
	<configInfo name="cel_amqp" language="en_US">
		<synopsis>AMQP CEL Backend</synopsis>
		<configFile name="cel_amqp.conf">
			<configObject name="global">
				<synopsis>Global configuration settings</synopsis>
				<configOption name="connection">
					<synopsis>Name of the connection from amqp.conf to use</synopsis>
					<description>
						<para>Specifies the name of the connection from amqp.conf to use</para>
					</description>
				</configOption>
				<configOption name="queue">
					<synopsis>Name of the queue to post to</synopsis>
					<description>
						<para>Defaults to asterisk_cel</para>
					</description>
				</configOption>
				<configOption name="exchange">
					<synopsis>Name of the exchange to post to</synopsis>
					<description>
						<para>Defaults to empty string</para>
					</description>
				</configOption>
			</configObject>
		</configFile>
	</configInfo>
 ***/

#include "asterisk.h"

#include "asterisk/stringfields.h"
#include "asterisk/cel.h"
#include "asterisk/channel.h"
#include "asterisk/config_options.h"
#include "asterisk/json.h"
#include "asterisk/module.h"
#include "asterisk/amqp.h"

#define CEL_NAME "AMQP"
#define CONF_FILENAME "cel_amqp.conf"

/*! \brief global config structure */
struct cel_amqp_global_conf {
	AST_DECLARE_STRING_FIELDS(
		/*! \brief connection name */
		AST_STRING_FIELD(connection);
		/*! \brief queue name */
		AST_STRING_FIELD(queue);
		/*! \brief exchange name */
		AST_STRING_FIELD(exchange);
	);

	/*! \brief current connection to amqp */
	struct ast_amqp_connection *amqp;
};

/*! \brief cel_amqp configuration */
struct cel_amqp_conf {
	struct cel_amqp_global_conf *global;
};

/*! \brief Locking container for safe configuration access. */
static AO2_GLOBAL_OBJ_STATIC(confs);

static struct aco_type global_option = {
	.type = ACO_GLOBAL,
	.name = "global",
	.item_offset = offsetof(struct cel_amqp_conf, global),
	.category = "^global$",
	.category_match = ACO_WHITELIST,
};

static struct aco_type *global_options[] = ACO_TYPES(&global_option);

static void conf_global_dtor(void *obj)
{
	struct cel_amqp_global_conf *global = obj;
	ao2_cleanup(global->amqp);
	ast_string_field_free_memory(global);
}

static struct cel_amqp_global_conf *conf_global_create(void)
{
	RAII_VAR(struct cel_amqp_global_conf *, global, NULL, ao2_cleanup);

	global = ao2_alloc(sizeof(*global), conf_global_dtor);
	if (!global) {
		return NULL;
	}

	if (ast_string_field_init(global, 64) != 0) {
		return NULL;
	}

	aco_set_defaults(&global_option, "global", global);

	return ao2_bump(global);
}

/*! \brief The conf file that's processed for the module. */
static struct aco_file conf_file = {
	/*! The config file name. */
	.filename = CONF_FILENAME,
	/*! The mapping object types to be processed. */
	.types = ACO_TYPES(&global_option),
};

static void conf_dtor(void *obj)
{
	struct cel_amqp_conf *conf = obj;

	ao2_cleanup(conf->global);
}

static void *conf_alloc(void)
{
	RAII_VAR(struct cel_amqp_conf *, conf, NULL, ao2_cleanup);

	conf = ao2_alloc_options(sizeof(*conf), conf_dtor,
		AO2_ALLOC_OPT_LOCK_NOLOCK);
	if (!conf) {
		return NULL;
	}

	conf->global = conf_global_create();
	if (!conf->global) {
		return NULL;
	}

	return ao2_bump(conf);
}

static int setup_amqp(void);

CONFIG_INFO_STANDARD(cfg_info, confs, conf_alloc,
	.files = ACO_FILES(&conf_file),
	.pre_apply_config = setup_amqp,
);

static int setup_amqp(void)
{
	struct cel_amqp_conf *conf = aco_pending_config(&cfg_info);

	if (!conf) {
		return 0;
	}

	if (!conf->global) {
		ast_log(LOG_ERROR, "Invalid cdr_amqp.conf\n");
		return -1;
	}

	/* Refresh the AMQP connection */
	ao2_cleanup(conf->global->amqp);
	conf->global->amqp = ast_amqp_get_connection(conf->global->connection);

	if (!conf->global->amqp) {
		ast_log(LOG_ERROR, "Could not get AMQP connection %s\n",
			conf->global->connection);
		return -1;
	}

	return 0;
}

/*!
 * \brief CEL handler for AMQP.
 *
 * \param event CEL event.
 */
static void amqp_cel_log(struct ast_event *event)
{
	RAII_VAR(struct cel_amqp_conf *, conf, NULL, ao2_cleanup);
	RAII_VAR(struct ast_json *, json, NULL, ast_json_unref);
	RAII_VAR(struct ast_json *, extra, NULL, ast_json_unref);
	RAII_VAR(char *, str, NULL, ast_json_free);
	const char *name;
	int res;
	struct ast_cel_event_record record = {
		.version = AST_CEL_EVENT_RECORD_VERSION,
	};
	amqp_basic_properties_t props = {
		._flags = AMQP_BASIC_DELIVERY_MODE_FLAG | AMQP_BASIC_CONTENT_TYPE_FLAG,
		.delivery_mode = 2, /* persistent delivery mode */
		.content_type = amqp_cstring_bytes("application/json")
	};

	conf = ao2_global_obj_ref(confs);

	ast_assert(conf && conf->global && conf->global->amqp);

	/* Extract the data from the CEL */
	if (ast_cel_fill_record(event, &record) != 0) {
		return;
	}

	/* Handle user define events */
	name = record.event_name;
	if (record.event_type == AST_CEL_USER_DEFINED) {
		name = record.user_defined_name;
	}

	/* Handle the optional extra field, although re-parsing JSON
	 * makes me sad :-( */
	if (strlen(record.extra) == 0) {
		extra = ast_json_null();
	} else {
		extra = ast_json_load_string(record.extra, NULL);
		if (!extra) {
			ast_log(LOG_ERROR, "Error parsing extra field\n");
			extra = ast_json_string_create(record.extra);
		}
	}

	json = ast_json_pack("{"
		/* event_name, account_code */
		"s: s, s: s,"
		/* num, name, ani, rdnis, dnid */
		"s: { s: s, s: s, s: s, s: s, s: s },"
		/* extension, context, channel, application */
		"s: s, s: s, s: s, s: s, "
		/* app_data, event_time, amaflags, unique_id */
		"s: s, s: o, s: s, s: s, "
		/* linked_id, uesr_field, peer, peer_account */
		"s: s, s: s, s: s, s: s, "
		/* extra */
		"s: o"
		"}",
		"event_name", name,
		"account_code", record.account_code,

		"caller_id",
		"num", record.caller_id_num,
		"name", record.caller_id_name,
		"ani", record.caller_id_ani,
		"rdnis", record.caller_id_rdnis,
		"dnid", record.caller_id_dnid,

		"extension", record.extension,
		"context", record.context,
		"channel", record.channel_name,
		"application", record.application_name,

		"app_data", record.application_data,
		"event_time", ast_json_timeval(record.event_time, NULL),
		"amaflags", ast_channel_amaflags2string(record.amaflag),
		"unique_id", record.unique_id,

		"linked_id", record.linked_id,
		"user_field", record.user_field,
		"peer", record.peer,
		"peer_acount", record.peer_account,
		"extra", extra);
	if (!json) {
		return;
	}

	/* Dump the JSON to a string for publication */
	str = ast_json_dump_string(json);
	if (!str) {
		ast_log(LOG_ERROR, "Failed to build string from JSON\n");
		return;
	}

	res = ast_amqp_basic_publish(conf->global->amqp,
		amqp_cstring_bytes(conf->global->exchange),
		amqp_cstring_bytes(conf->global->queue),
		0, /* mandatory; don't return unsendable messages */
		0, /* immediate; allow messages to be queued */
		&props,
		amqp_cstring_bytes(str));

	if (res != 0) {
		ast_log(LOG_ERROR, "Error publishing CEL to AMQP\n");
	}
}

static int load_config(int reload)
{
	RAII_VAR(struct cel_amqp_conf *, conf, NULL, ao2_cleanup);
	RAII_VAR(struct ast_amqp_connection *, amqp, NULL, ao2_cleanup);

	switch (aco_process_config(&cfg_info, reload)) {
	case ACO_PROCESS_ERROR:
		return -1;
	case ACO_PROCESS_OK:
	case ACO_PROCESS_UNCHANGED:
		break;
	}

	conf = ao2_global_obj_ref(confs);
	if (!conf || !conf->global) {
		ast_log(LOG_ERROR, "Error obtaining config from cel_amqp.conf\n");
		return -1;
	}

	/* Refresh the AMQP connection */
	ao2_cleanup(conf->global->amqp);
	conf->global->amqp = ast_amqp_get_connection(conf->global->connection);

	if (!conf->global->amqp) {
		ast_log(LOG_ERROR, "Could not get AMQP connection %s\n",
			conf->global->connection);
		return -1;
	}

	return 0;
}

static int load_module(void)
{
	if (aco_info_init(&cfg_info) != 0) {
		ast_log(LOG_ERROR, "Failed to initialize config");
		aco_info_destroy(&cfg_info);
		return -1;
	}

	aco_option_register(&cfg_info, "connection", ACO_EXACT,
		global_options, "", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct cel_amqp_global_conf, connection));
	aco_option_register(&cfg_info, "queue", ACO_EXACT,
		global_options, "asterisk_cel", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct cel_amqp_global_conf, queue));
	aco_option_register(&cfg_info, "exchange", ACO_EXACT,
		global_options, "", OPT_STRINGFIELD_T, 0,
		STRFLDSET(struct cel_amqp_global_conf, exchange));

	if (load_config(0) != 0) {
		ast_log(LOG_WARNING, "Configuration failed to load\n");
		return AST_MODULE_LOAD_DECLINE;
	}

	if (ast_cel_backend_register(CEL_NAME, amqp_cel_log) != 0) {
		ast_log(LOG_ERROR, "Could not register CEL backend\n");
		return AST_MODULE_LOAD_FAILURE;
	}

	ast_log(LOG_NOTICE, "CEL AMQP logging enabled\n");
	return AST_MODULE_LOAD_SUCCESS;
}

static int unload_module(void)
{
	aco_info_destroy(&cfg_info);
	ao2_global_obj_release(confs);
	if (ast_cel_backend_unregister(CEL_NAME) != 0) {
		return -1;
	}

	return 0;
}

static int reload_module(void)
{
	return load_config(1);
}

AST_MODULE_INFO(ASTERISK_GPL_KEY, AST_MODFLAG_LOAD_ORDER, "AMQP CEL Backend",
		.support_level = AST_MODULE_SUPPORT_CORE,
		.load = load_module,
		.unload = unload_module,
		.reload = reload_module,
		.load_pri = AST_MODPRI_CDR_DRIVER,
	);