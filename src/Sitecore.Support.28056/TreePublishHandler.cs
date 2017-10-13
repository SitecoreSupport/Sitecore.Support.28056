using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Sitecore.Framework.Eventing;
using Sitecore.Framework.Publishing.DataPromotion;
using Sitecore.Framework.Publishing.ManifestCalculation;
using Sitecore.Framework.Publishing.PublisherOperations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using Microsoft.AspNetCore.Hosting;
using System.Threading.Tasks;
using Sitecore.Framework.Publishing.Data;
using Sitecore.Framework.Publishing.PublishJobQueue.Handlers;
using Sitecore.Framework.Publishing;
using Sitecore.Framework.Publishing.PublishJobQueue;

namespace Sitecore.Support.Framework.Publishing.PublishJobQueue.Handlers
{
  public class TreePublishHandler : BaseHandler
  {
    public TreePublishHandler(
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
    {

    }

    public TreePublishHandler(
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
      var startNode = publishSourceRepository.GetNode(publishContext.PublishOptions.ItemId.Value).Result;

      if (startNode == null)
        throw new ArgumentNullException($"The publish could not be performed from a start item that doesn't exist : {publishContext.PublishOptions.ItemId.Value}.");

      var parentNode = startNode.ParentId != null ?
          publishSourceRepository.GetNode(startNode.ParentId.Value).Result :
          startNode;

      ISourceObservable<CandidateValidationContext> publishSourceStream = new TreeNodeSourceProducer(
          publishSourceRepository,
          startNode,
          validator,
          publishContext.PublishOptions.Descendants,
          _options.SourceTreeReaderBatchSize,
          errorSource,
          _loggerFactory.CreateLogger<TreeNodeSourceProducer>(),
          _loggerFactory.CreateLogger<DiagnosticLogger>());

      if (publishContext.PublishOptions.GetItemBucketsEnabled() && parentNode.Node.Properties.TemplateId == publishContext.PublishOptions.GetBucketTemplateId())
      {
        publishSourceStream = new BucketNodeSourceProducer(
            publishSourceStream,
            publishSourceRepository,
            startNode,
            publishContext.PublishOptions.GetBucketTemplateId(),
            errorSource,
            _loggerFactory.CreateLogger<BucketNodeSourceProducer>(),
            _loggerFactory.CreateLogger<DiagnosticLogger>());
      }

      return publishSourceStream;
    }

    protected override IObservable<CandidateValidationTargetContext> CreateTargetProcessingStream(
        PublishContext publishContext,
        IPublishCandidateSource publishSourceRepository,
        IPublishValidator validator,
        IObservable<CandidateValidationContext> publishStream,
        ITargetItemIndexService targetIndex,
        IRequiredPublishFieldsResolver requiredPublishFieldsResolver,
        CancellationTokenSource errorSource,
        Guid targetId)
    {
      //   Source items -Create target publish stream->PublishCandidateTargetContext
      IPublishCandidateTargetValidator parentValidator = null;
      if (publishContext.PublishOptions.GetItemBucketsEnabled())
      {
        parentValidator = new PublishTargetBucketParentValidator(publishSourceRepository, targetIndex, publishContext.PublishOptions.GetBucketTemplateId());
      }
      else
      {
        parentValidator = new PublishTargetParentValidator(publishSourceRepository, targetIndex);
      }

      publishStream = new CandidatesParentValidationTargetProducer(
          publishStream,
          parentValidator,
          errorSource,
          publishContext.SourceStore.GetItemReadRepository(),
          _loggerFactory.CreateLogger<CandidatesParentValidationTargetProducer>(),
          _loggerFactory.CreateLogger<DiagnosticLogger>());

      publishStream = new CandidatesValidationTargetProducer(
          publishStream,
          validator,
          targetId,
          errorSource,
          _loggerFactory.CreateLogger<CandidatesValidationTargetProducer>(),
           _loggerFactory.CreateLogger<DiagnosticLogger>());

      if (_options.DeleteOphanedItems && publishContext.PublishOptions.Descendants)
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

      return base.CreateTargetProcessingStream(
          publishContext,
          publishSourceRepository,
          validator,
          publishStream,
          targetIndex,
          requiredPublishFieldsResolver,
          errorSource,
          targetId);
    }

    #endregion

    public override bool CanHandle(PublishJob job, PublishContext publishContext) => job.Options.ItemId.HasValue;

    protected override async Task UpdateTargetSyncState(PublishContext context, IEnumerable<IManifestOperationResult> promotionResults)
    {
      if (context.PublishOptions.ItemId == PublishingConstants.ItemTreeRootId && context.PublishOptions.Descendants)
      {
        await base.UpdateTargetSyncState(context, promotionResults).ConfigureAwait(false);
      }
    }
  }
}
