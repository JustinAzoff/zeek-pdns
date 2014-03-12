@load base/protocols/dns
@load ./dns-ans-query

module PDNS;

export {
    # path to bro_pdns.py script.  only matters if you process locally
    const tool = "/bro/tools/bro_pdns.py" &redef;
    # define URI where DB lives
    const uri = "sqlite:////bro/logs/dns.db" &redef;
    # true if you want logs scp'd to remote host for processing
    # if you use scp you must set upload variables
    const use_scp = T &redef;
    # upload or rotation interval
    const log_interval = 5min &redef;
    # variables to use when processing remotely and scp is required
    const upload_user = "someuser" &redef;
    const upload_host = "some.host.edu" &redef;
    const upload_path = "path/forlogs" &redef;
}

# process DNS logs
function process_log(info: Log::RotationInfo) : bool
{

    local cmd = fmt("BRO_PDNS_DB=%s %s process %s && rm %s", uri, tool, info$fname, info$fname);
    when (local res = Exec::run([$cmd=cmd])) {
        ## do nothing
    }
    return T;
}

event bro_init()
{
    if ( use_scp )
      {
        Log::add_filter(DNS::LOG, [
          $name="dns-passivedns",
          $path="dns-passivedns",
          $interv=log_interval,
          $postprocessor=Log::sftp_postprocessor]);
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
