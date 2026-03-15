using FluentAssertions;
using SqlClone.Domain.Models;

namespace SqlClone.Tests;

public sealed class ValidationResultTests
{
    [Fact]
    public void IsSuccessful_WhenAllChecksPass()
    {
        var result = new ValidationResult
        {
            SqlReachable = true,
            Databases = new Dictionary<string, bool> { ["AppDb"] = true },
            LinkedServers = new Dictionary<string, bool> { ["REMOTE"] = true }
        };

        result.IsSuccessful.Should().BeTrue();
    }

    [Fact]
    public void IsSuccessful_False_WhenAnyCheckFails()
    {
        var result = new ValidationResult
        {
            SqlReachable = true,
            Databases = new Dictionary<string, bool> { ["AppDb"] = false },
            LinkedServers = new Dictionary<string, bool> { ["REMOTE"] = true }
        };

        result.IsSuccessful.Should().BeFalse();
    }
}
