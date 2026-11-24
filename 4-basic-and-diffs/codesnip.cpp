std::string RedisJobStatus::resultToString() const
{
    switch (result)
    {
        case Result::SUCCESS: return "success";
        case Result::NOOP:    return "noop";
        case Result::FAILURE: return "failure";
        default:              return "unknown";
    }
}
