using System;
using System.Reactive.Linq;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Sitecore.Framework.Eventing;
using Sitecore.Framework.Publishing.DataPromotion;
using Sitecore.Framework.Publishing.ManifestCalculation;
using Sitecore.Framework.Publishing.PublisherOperations;
using System.Threading;

namespace Sitecore.Framework.Publishing.PublishJobQueue.Handlers
{
  using System.Linq;
  using Sitecore.Framework.Publishing.Data;

  public class IncrementalPublishHandler : BaseHandler
  {
    public IncrementalPublishHandler(
        IRequiredPublishFieldsResolver requiredPublishFieldsResolver,
        IPublisherOperationService publisherOpsService,
        IPromotionCoordinator promoterCoordinator,
        IEventRegistry eventRegistry,
        ILoggerFactory loggerFactory,
        IApplicationLifetime applicationLifetime,
        PublishJobHandlerOptions options = null) : base(
            requiredPublishFieldsResolver,
            publisherOpsService,
            promoterCoordinator,
            eventRegistry,
            loggerFactory,
            applicationLifetime,
            options ?? new PublishJobHandlerOptions())
    { }

    public IncrementalPublishHandler(
        IRequiredPublishFieldsResolver requiredPublishFieldsResolver,
        IPublisherOperationService publisherOpsService,
        IPromotionCoordinator promoterCoordinator,
        IEventRegistry eventRegistry,
        ILoggerFactory loggerFactory,
        IApplicationLifetime applicationLifetime,
        IConfiguration config) : this(
            requiredPublishFieldsResolver,
            publisherOpsService,
            promoterCoordinator,
            eventRegistry,
            loggerFactory,
            applicationLifetime,
            config.As<PublishJobHandlerOptions>())
    { }

    #region Factories

    protected override ISourceObservable<CandidateValidationContext> CreatePublishSourceStream(
        PublishContext publishContext,
        IPublishCandidateSource publishSourceRepository,
        IPublishValidator validator,
        IPublisherOperationService publisherOperationService,
        CancellationTokenSource errorSource)
    {
      var targetNames = publishContext.PublishOptions.GetTargets().ToArray();
      var targetStores = publishContext.TargetStores.Where(s => targetNames.Contains(s.Name));

      var targets = targetStores.ToDictionary(s => s.Id, s => s.Name);

      var publishStreamSource = new UnpublishedNodeSourceProducer(
          publishContext.Started,
          publishContext.PublishOptions.Languages,
          targets,
          new string[] { publishContext.PublishOptions.GetPublishType() },
          publishSourceRepository,
          publisherOperationService,
          validator,
          _options.UnpublishedOperationsLoadingBatchSize,
          errorSource,
          errorSource.Token,
          _loggerFactory.CreateLogger<UnpublishedNodeSourceProducer>(),
          _loggerFactory.CreateLogger<DiagnosticLogger>());

      return new DeletedNodesSourceProducer(
          publishStreamSource,
          publishContext.Started,
          publishContext.PublishOptions.Languages,
          publishContext.PublishOptions.Targets,
          new string[] { publishContext.PublishOptions.GetPublishType() },
          publisherOperationService,
          _options.UnpublishedOperationsLoadingBatchSize,
          errorSource,
          _loggerFactory.CreateLogger<DeletedNodesSourceProducer>(),
          _loggerFactory.CreateLogger<DiagnosticLogger>());
    }
    #endregion

    public override bool CanHandle(PublishJob job, PublishContext publishContext) => !job.Options.ItemId.HasValue;

    protected override IObservable<CandidateValidationTargetContext> CreateTargetProcessingStream(PublishContext publishContext, IPublishCandidateSource publishSourceRepository, IPublishValidator validator, IObservable<CandidateValidationContext> publishStream, ITargetItemIndexService targetIndex, IRequiredPublishFieldsResolver requiredPublishFieldsResolver, CancellationTokenSource errorSource, Guid targetId)
    {
      publishStream = new CandidatesValidationTargetProducer(
          publishStream,
          validator,
          targetId,
          errorSource,
          _loggerFactory.CreateLogger<CandidatesValidationTargetProducer>(),
           _loggerFactory.CreateLogger<DiagnosticLogger>());

      if (_options.DeleteOphanedItems)
      {
        var orphanStream = new OrphanedItemValidationTargetProducer(publishStream,
            targetIndex,
            publishContext.SourceStore.GetItemReadRepository(),
            _options,
            errorSource,
            _loggerFactory.CreateLogger<OrphanedItemValidationTargetProducer>(),
             _loggerFactory.CreateLogger<DiagnosticLogger>());

        publishStream = publishStream.Merge(orphanStream);
      }

      return base.CreateTargetProcessingStream(publishContext, publishSourceRepository, validator, publishStream, targetIndex, requiredPublishFieldsResolver, errorSource, targetId);
    }
  }
}
