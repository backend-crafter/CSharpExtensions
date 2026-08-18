using System.Net.Mime;
using CSharpExtensions.AspNetCore.AspNet.Extensions;
using Microsoft.AspNetCore.Mvc.Formatters;

namespace CSharpExtensions.Kafka.Core;

using System;
using System.Collections.Concurrent;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CSharpExtensions.Kafka.Abstractions;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;

/// <summary>
/// HTTP endpoints for Kafka infrastructure maintenance and diagnostics.
/// Registered only when <c>kafka.UseMaintenanceEndpoints()</c> is called.
/// </summary>
[Authorize(Policy = KafkaMaintenancePolicies.Read)]
[ApiController]
[Route("api/v1/kafka-maintenance")]
[Produces(MediaTypeNames.Application.ProblemJson)]
[ApiExplorerSettings(GroupName = "Kafka Maintenance")]
public sealed class KafkaMaintenanceController(
    IKafkaMaintenanceService maintenanceService,
    KafkaRecoveryManager recoveryManager) : ControllerBase
{
    /// <summary>
    /// Replays messages from a DLQ topic back to the original source topic.
    /// </summary>
    /// <param name="topicConfigurationKey">Topic configuration key (e.g., <c>"SmsCampaignDispatched"</c>).</param>
    /// <param name="maxMessages">Max messages to replay in a single batch.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [HttpPost("replay-dlq/{topicConfigurationKey}")]
    [Authorize(Policy = KafkaMaintenancePolicies.Write)]
    [ProducesResponseType(typeof(int), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<ActionResult<int>> ReplayDlq(
        [FromRoute, StringLength(200, MinimumLength = 1)] string topicConfigurationKey,
        [FromQuery, Range(1, 10000)] int maxMessages = 1000,
        CancellationToken cancellationToken = default)
    {
        var result = await maintenanceService.ReplayDlqAsync(topicConfigurationKey, maxMessages, cancellationToken);
        return result.ToActionResult();
    }

    /// <summary>
    /// Purges stale or incomplete message assembly segments past their retention period.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    [HttpPost("purge-assemblies")]
    [Authorize(Policy = KafkaMaintenancePolicies.Write)]
    [ProducesResponseType(typeof(int), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<ActionResult<int>> PurgeStaleAssemblies(CancellationToken cancellationToken = default)
    {
        var result = await maintenanceService.PurgeStaleAssembliesAsync(cancellationToken);
        return result.ToActionResult();
    }

    /// <summary>
    /// Retries all dead-lettered staged jobs of a specific type by resetting them to Pending.
    /// </summary>
    /// <param name="jobType">Job type identifier (e.g., <c>"ResolveWagerFact"</c>).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [HttpPost("retry-jobs/{jobType}")]
    [Authorize(Policy = KafkaMaintenancePolicies.Write)]
    [ProducesResponseType(typeof(int), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<ActionResult<int>> RetryDeadLetteredJobs(
        [FromRoute, StringLength(200, MinimumLength = 1)] string jobType,
        CancellationToken cancellationToken = default)
    {
        var result = await maintenanceService.RetryDeadLetteredJobsAsync(jobType, cancellationToken);
        return result.ToActionResult();
    }

    /// <summary>
    /// Validates topic data integrity by scanning a sample of recent messages.
    /// </summary>
    /// <param name="topicConfigurationKey">Topic configuration key (e.g., <c>"SmsCampaignDispatched"</c>).</param>
    /// <param name="sampleSize">Max messages to scan.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [HttpGet("validate/{topicConfigurationKey}")]
    [ProducesResponseType(typeof(TopicValidationReport), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<ActionResult<TopicValidationReport>> ValidateTopic(
        [FromRoute, StringLength(200, MinimumLength = 1)] string topicConfigurationKey,
        [FromQuery, Range(1, 1000)] int sampleSize = 100,
        CancellationToken cancellationToken = default)
    {
        var result = await maintenanceService.ValidateTopicAsync(topicConfigurationKey, sampleSize, cancellationToken);
        return result.ToActionResult();
    }

    /// <summary>
    /// Retrieves metadata for a Kafka topic (partitions, replication, ISR status).
    /// </summary>
    /// <param name="topicName">Physical Kafka topic name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    [HttpGet("topic-metadata/{topicName}")]
    [ProducesResponseType(typeof(TopicMetadata), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<ActionResult<TopicMetadata>> GetTopicMetadata(
        [FromRoute, StringLength(249, MinimumLength = 1)] string topicName,
        CancellationToken cancellationToken = default)
    {
        var result = await maintenanceService.GetTopicMetadataAsync(topicName, cancellationToken);
        return result.ToActionResult();
    }

    /// <summary>
    /// Returns the count of pending outbox records not yet published to Kafka.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    [HttpGet("outbox-pending")]
    [ProducesResponseType(typeof(int), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<ActionResult<int>> GetPendingOutboxCount(CancellationToken cancellationToken = default)
    {
        var result = await maintenanceService.GetPendingOutboxCountAsync(cancellationToken);
        return result.ToActionResult();
    }

    /// <summary>
    /// Rebuilds database indexes for Kafka outbox, staged jobs, and message assembly tables.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    [HttpPost("rebuild-indexes")]
    [Authorize(Policy = KafkaMaintenancePolicies.Write)]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<ActionResult> RebuildIndexes(CancellationToken cancellationToken = default)
    {
        var result = await maintenanceService.RebuildIndexesAsync(cancellationToken);
        return result.ToActionResult();
    }

    /// <summary>
    /// Starts background topic recovery for all registered repair configurations.
    /// </summary>
    [HttpPost("recovery/start")]
    [Authorize(Policy = KafkaMaintenancePolicies.Write)]
    [ProducesResponseType(typeof(IReadOnlyCollection<KafkaRecoveryStatus>), StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public ActionResult<IReadOnlyCollection<KafkaRecoveryStatus>> StartRecovery()
    {
        var result = recoveryManager.StartAllRecoveries();
        if (!result.IsSuccess)
        {
            return BadRequest(CreateProblemDetails(
                StatusCodes.Status400BadRequest,
                "Kafka.RecoveryStartRejected",
                "Kafka recovery could not be started."));
        }
        return AcceptedAtAction(nameof(GetRecoveryStatuses), recoveryManager.GetStatuses());
    }

    /// <summary>
    /// Starts background topic recovery for a specific configuration by extracting historical messages
    /// from a legacy source topic and publishing them to the target topic.
    /// </summary>
    /// <param name="topicConfigurationKey">Topic configuration key (e.g., <c>"EligibleWagerFactRecorded"</c>).</param>
    /// <param name="sourceTopicName">Physical name of the legacy source topic to extract from (e.g., <c>"wager.events.eligible-fact.recorded"</c>).</param>
    [HttpPost("recovery/start-from-source/{topicConfigurationKey}")]
    [Authorize(Policy = KafkaMaintenancePolicies.Write)]
    [ProducesResponseType(typeof(KafkaRecoveryStatus), StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult<KafkaRecoveryStatus> StartRecoveryFromSource(
        [FromRoute, StringLength(200, MinimumLength = 1)] string topicConfigurationKey,
        [FromQuery, StringLength(249, MinimumLength = 1)] string sourceTopicName)
    {
        var result = recoveryManager.StartRecoveryFromSource(topicConfigurationKey, sourceTopicName);
        if (!result.IsSuccess)
        {
            return BadRequest(CreateProblemDetails(
                StatusCodes.Status400BadRequest,
                "Kafka.RecoveryStartRejected",
                "Kafka recovery could not be started."));
        }

        var targetTopicName = recoveryManager.ResolveTopicName(topicConfigurationKey);
        var status = recoveryManager.GetStatuses()
            .FirstOrDefault(s => string.Equals(s.TopicName, targetTopicName, StringComparison.OrdinalIgnoreCase));

        if (status == null)
        {
            return NotFound(CreateProblemDetails(
                StatusCodes.Status404NotFound,
                "Kafka.RecoveryNotFound",
                "No active Kafka recovery process was found."));
        }

        return AcceptedAtAction(nameof(GetRecoveryStatus), new { topicName = targetTopicName }, status);
    }

    /// <summary>
    /// Gets the status of all registered background topic recovery processes.
    /// </summary>
    [HttpGet("recovery/status")]
    [ProducesResponseType(typeof(IReadOnlyCollection<KafkaRecoveryStatus>), StatusCodes.Status200OK)]
    public ActionResult<IReadOnlyCollection<KafkaRecoveryStatus>> GetRecoveryStatuses()
    {
        return Ok(recoveryManager.GetStatuses());
    }

    /// <summary>
    /// Gets the status of the background recovery process for the specified topic.
    /// </summary>
    /// <param name="topicName">The name of the topic.</param>
    [HttpGet("recovery/status/{topicName}")]
    [ProducesResponseType(typeof(KafkaRecoveryStatus), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public ActionResult<KafkaRecoveryStatus> GetRecoveryStatus(
        [FromRoute, StringLength(249, MinimumLength = 1)] string topicName)
    {
        var status = recoveryManager.GetStatuses()
            .FirstOrDefault(s => string.Equals(s.TopicName, topicName, StringComparison.OrdinalIgnoreCase));

        if (status != null)
        {
            return Ok(status);
        }
        return NotFound(CreateProblemDetails(
            StatusCodes.Status404NotFound,
            "Kafka.RecoveryNotFound",
            "No active Kafka recovery process was found."));
    }

    private static ProblemDetails CreateProblemDetails(int status, string type, string title)
    {
        return new ProblemDetails
        {
            Status = status,
            Type = type,
            Title = title
        };
    }
}
