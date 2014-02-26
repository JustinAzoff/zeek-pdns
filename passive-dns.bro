@load base/protocols/dns
@load dns-ans-query

module PDNS;

export {
    const tool = "/bro/tools/bro_pdns.py" &redef;
    const uri = "sqlite:////bro/logs/dns.db" &redef;
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
    Log::add_filter(DNS::LOG, [
        $name="dns-passivedns",
        $path="dns-passivedns",
        $interv=60sec,
        $postprocessor=process_log
    ]);
}
