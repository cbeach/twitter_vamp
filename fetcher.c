/* twitter feed fetcher
   
 This program downloads a stream of json-encoded tweets, breaks them apart on
 '\r', and sends each as an amqp message with the routing key "raw" to the
 exchange "amq.direct
 
 This file contains code licsensed under either MPL 1.1 or GPL2.0 (or later).
 
 License: GNU General Public License, version 3 or later */

#include <curl/curl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <amqp.h>
#include <amqp_framing.h>

#define FETCHER_AMQP_ROUTING_KEY "raw"
#define FETCHER_AMQP_EXCHANGE "amq.direct"
#define FETCHER_AMQP_CHANNEL 1
#define DEFAULT_AMQP_SERVER_ADDR "localhost"
#define DEFAULT_AMQP_SERVER_PORT 5672

#define BUFFER_SIZE sizeof(char)
#define BUFFER_NMEMB 1048576
typedef struct {
  /* data starts at buffer, last char at end-1 */
  char* buffer;
  char* end;
  amqp_connection_state_t conn;
  amqp_bytes_t exchange;
  amqp_bytes_t routing_key;
} write_callback_data;

/* todo: retry amqp connection rather than simply dieing */
/* taken from http://hg.rabbitmq.com/rabbitmq-c/file/fb6fca832fd2/examples/utils.c */
void die_on_error(int x, char const *context) {
  if (x < 0) {
    char *errstr = amqp_error_string(-x);
    fprintf(stderr, "%s: %s\n", context, errstr);
    free(errstr);
    exit(EXIT_FAILURE);
  }
}

/* todo: retry amqp connection rather than simply dieing */
/* taken from http://hg.rabbitmq.com/rabbitmq-c/file/fb6fca832fd2/examples/utils.c */
void die_on_amqp_error(amqp_rpc_reply_t x, char const *context) {
  switch (x.reply_type) {
    case AMQP_RESPONSE_NORMAL:
      return;
      
    case AMQP_RESPONSE_NONE:
      fprintf(stderr, "%s: missing RPC reply type!\n", context);
      break;
      
    case AMQP_RESPONSE_LIBRARY_EXCEPTION:
      fprintf(stderr, "%s: %s\n", context, amqp_error_string(x.library_error));
      break;
      
    case AMQP_RESPONSE_SERVER_EXCEPTION:
      switch (x.reply.id) {
	case AMQP_CONNECTION_CLOSE_METHOD: {
	  amqp_connection_close_t *m = (amqp_connection_close_t *) x.reply.decoded;
	  fprintf(stderr, "%s: server connection error %d, message: %.*s\n",
		  context,
		  m->reply_code,
		  (int) m->reply_text.len, (char *) m->reply_text.bytes);
	  break;
	}
	case AMQP_CHANNEL_CLOSE_METHOD: {
	  amqp_channel_close_t *m = (amqp_channel_close_t *) x.reply.decoded;
	  fprintf(stderr, "%s: server channel error %d, message: %.*s\n",
		  context,
		  m->reply_code,
		  (int) m->reply_text.len, (char *) m->reply_text.bytes);
	  break;
	}
	default:
	  fprintf(stderr, "%s: unknown server error, method id 0x%08X\n", context, x.reply.id);
	  break;
      }
      break;
  }
  
  exit(EXIT_FAILURE);
}

/* Send a message with body @start of length @size according to the amqp
 information in @data. */
void send_message(write_callback_data* data, char* start, size_t size) {
  amqp_bytes_t message;
  message.len = size;
  message.bytes = start;
  
  die_on_error(
    amqp_basic_publish(
      /* amqp_connection_state_t state              */ data->conn,
      /* amqp_channel_t channel                     */ FETCHER_AMQP_CHANNEL,
      /* amqp_bytes_t exchange                      */ data->exchange,
      /* amqp_bytes_t routing_key                   */ data->routing_key,
      /* amqp_boolean_t mandatory                   */ 0,
      /* amqp_boolean_t immediate                   */ 0,
      /* amqp_basic_properties_t_ const *properties */ NULL,
      /* amqp_bytes_t body                          */ message),
    "Publishing");
}

#define SPLIT_CHARACTER '\r'
/* Called by curl when some data is available. @start contains @nmemb characters
 (each of @size bytes); @userdata is a pointer to our previously set
 write_callback_data.  See also
 http://curl.haxx.se/libcurl/c/curl_easy_setopt.html#CURLOPTWRITEFUNCTION .
 
 The goal of this function is to call send_message() on blocks of data delimited
 by SPLIT_CHARACTER. A block may be split over multiple calls to this function.
 Accordingly, it keeps a buffer in @userdata of all the previously-seen data in
 this block and combines it with the newly given data.  Once it finds a
 SPLIT_CHARACTER, the data in the buffer is sent via send_message(). If an
 entire block of data is contained in @start, the buffer is not used. */
size_t write_callback(char *start, size_t size, size_t nmemb, void *userdata) {
  size_t processed = size * nmemb; /* returning less than this signals an error
                                      to curl */
  write_callback_data* data = (write_callback_data*) userdata;
  char* end = start + nmemb;
  char* split = start; /* location of next SPLIT_CHARACTER */
  char* cur = start;   /* start of unprocessed data */
  size_t remaining;

  if (BUFFER_SIZE != size) {
    fprintf(stderr, "Unexpected data size! Expected: %i; found: %i\n",
      BUFFER_SIZE, size);
    exit(EXIT_FAILURE);
  }

  while (NULL != split) {
    if (cur == start && data->buffer != data->end ) { /* use data in buffer */
      if (nmemb > BUFFER_NMEMB - (data->end - data->buffer)) {
        fprintf(stderr, "Out of buffer space! Have %i, need %i.\n",
          BUFFER_NMEMB * BUFFER_SIZE,
          BUFFER_SIZE * (data->end - data->buffer) + processed);
        /* todo: skip current block rather than exit */
        exit(EXIT_FAILURE);
      }
      split = (char*) memccpy(data->end, cur, SPLIT_CHARACTER, processed);
      if (NULL == split) { /* end of data */
        data->end += nmemb;
      } else {
        cur += split - data->end;
        data->end = split;
        send_message(data, data->buffer, data->end - data->buffer);
        data->end = data->buffer;
      }
    } else { /* all data is in ptr, not in buffer */
      split = memchr(cur, SPLIT_CHARACTER, end-cur);
      if (NULL == split) { /* end of data; put in buffer */
        remaining = end-cur;
        if (end-cur > BUFFER_NMEMB) {
          fprintf(stderr, "Not enough buffer space! Have %i, need %i.\n",
            BUFFER_NMEMB * BUFFER_SIZE, size * remaining);
          exit(EXIT_FAILURE);
        }
        memcpy(data->buffer, cur, size * remaining);
        data->end = data->buffer + remaining;
      } else { /* more data */
        split++; /* include SPLIT_CHARACTER */
        send_message(data, cur, split - cur);
        cur = split;
      }
    }
  }
  return processed;
}

int main(int argc, char** argv) {
  CURL *curl;
  CURLcode res;

  int sockfd;
  /* todo: accept amqp server connection info on cmd line */
  char const* hostname = DEFAULT_AMQP_SERVER_ADDR; /* amqp server address */
  int port = DEFAULT_AMQP_SERVER_PORT; /* default amqp server port */

  curl = curl_easy_init();
  if (NULL != curl) {
    char* errorbuf = malloc(CURL_ERROR_SIZE);
    write_callback_data data;
    data.buffer = calloc(BUFFER_NMEMB, BUFFER_SIZE);
    if (NULL == data.buffer) {
      fprintf(stderr, "Unablle to allocate buffer with %i elements of size %i", BUFFER_NMEMB, BUFFER_SIZE);
      exit(EXIT_FAILURE);
    }
    data.end = data.buffer;

    { /* amqp setup */
      data.conn = amqp_new_connection();
      data.exchange = amqp_cstring_bytes(FETCHER_AMQP_EXCHANGE);
      data.routing_key = amqp_cstring_bytes(FETCHER_AMQP_ROUTING_KEY);
      die_on_error(sockfd = amqp_open_socket(hostname, port), "Opening socket");
      amqp_set_sockfd(data.conn, sockfd);
      die_on_amqp_error(
        amqp_login(data.conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest"),
        "Logging in");
      amqp_channel_open(data.conn, FETCHER_AMQP_CHANNEL);
      die_on_amqp_error(amqp_get_rpc_reply(data.conn), "Opening channel");
    }
    { /* curl setup */
      curl_easy_setopt(curl, CURLOPT_URL, "http://owenja.dyndns.org/files/sample.10000.json");
      curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
      curl_easy_setopt(curl, CURLOPT_WRITEDATA, &data);
      curl_easy_setopt(curl, CURLOPT_ERRORBUFFER, errorbuf);
      /* curl_easy_setopt(curl, CURLOPT_USERPWD, "username:password");
          OR
         curl_easy_setopt(curl, CURLOPT_USERNAME, "username");
         curl_easy_setopt(curl, CURLOPT_PASSWORD, "password"); */
    }
    
    { /* todo: while loop and handle signal */
      res = curl_easy_perform(curl);
      if (0 != res) {
        fprintf(stderr, "%s", errorbuf);
      }
    }
    curl_easy_cleanup(curl);
    die_on_amqp_error(amqp_channel_close(data.conn, 1, AMQP_REPLY_SUCCESS), "Closing channel");
    die_on_amqp_error(amqp_connection_close(data.conn, AMQP_REPLY_SUCCESS), "Closing connection");
    die_on_error(amqp_destroy_connection(data.conn), "Ending connection");
    free(errorbuf);
    free(data.buffer);
  }
  return 0;
}
