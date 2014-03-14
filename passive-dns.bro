@load base/protocols/dns
@load ./dns-ans-query

module PDNS;

export {
    # path to bro_pdns.py script.  only matters if you process locally
    const tool = "/bro/tools/bro_pdns.py" &redef;
    # define URI where DB lives
    const uri = "sqlite:////bro/logs/dns.db" &redef;
    # true if you want logs sftp'd to remote host for processing
    # if you use sftp you must set upload variables
    const use_sftp = F &redef;
    # upload or rotation interval
    const log_interval = 5min &redef;
    # variables to use when processing remotely and sftp is required
    const upload_user = "someuser" &redef;
    const upload_host = "some.host.edu" &redef;
    const upload_path = "path/forlogs" &redef;

    redef enum Log::ID += { LOG };
    type Info: record {
            ## Timestamp when the log line was finished and written.
            ts:         time   &log;
            ## The number of unique dns query/response pairs
            records:    count  &log;
            ## The stderr from the process
            err:        vector of string &log &optional;
    };
}

# process DNS logs
function process_log(info: Log::RotationInfo) : bool
{

    local cmd = fmt("BRO_PDNS_DB=%s %s process %s && rm %s", uri, tool, info$fname, info$fname);
    when (local res = Exec::run([$cmd=cmd])) {
        local l: Info;
        l$ts      = network_time();
        l$records = 0;
        if(res?$stdout) {
            l$records = to_count(res$stdout[0]);
        }
        if(res?$stderr) {
            l$err = res$stderr;
        }
        Log::write(LOG, l);
    }
    return T;
}

event bro_init()
{
    Log::create_stream(LOG, [$columns=Info]);
    if ( use_sftp )
      {
        Log::add_filter(DNS::LOG, [
          $name="dns-passivedns",
          $path="dns-passivedns",
          $interv=log_interval,
          $postprocessor=Log::sftp_postprocessor
        ]);
        Log::sftp_destinations[Log::WRITER_ASCII,"dns-passivedns"] = set([$user=upload_user,$host=upload_host,$path=upload_path]);
      }
    else
      {
        Log::add_filter(DNS::LOG, [
          $name="dns-passivedns",
          $path="dns-passivedns",
          $interv=log_interval,
          $postprocessor=process_log
        ]);
      }
}
