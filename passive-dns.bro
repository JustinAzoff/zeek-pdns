@load base/protocols/dns
@load dns-ans-query

# process DNS logs
function process_log(info: Log::RotationInfo) : bool
{

    local cmd = fmt("/bro/tools/process_dns.py %s && rm %s", info$fname, info$fname);
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
