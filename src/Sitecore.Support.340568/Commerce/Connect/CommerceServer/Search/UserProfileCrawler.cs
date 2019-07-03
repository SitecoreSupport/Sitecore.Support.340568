using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Sitecore.Commerce.Connect.CommerceServer.Search;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.Diagnostics;
using Sitecore.SecurityModel;
using Sitecore.Sites;
using Debug = System.Diagnostics.Debug;

namespace Sitecore.Support.Commerce.Connect.CommerceServer.Search
{
    public class UserProfileCrawler : Sitecore.Commerce.Connect.CommerceServer.Search.UserProfileCrawler
    {
        private readonly IDictionary<Guid, CrawlState<CommerceProfileIndexableItem>> currentCrawlOperations = new ConcurrentDictionary<Guid, CrawlState<CommerceProfileIndexableItem>>();

        /// <summary>Called when the stop has indexing.</summary>
        protected override void OnStopIndexing()
        {
            currentCrawlOperations.Values.ToList().ForEach(c => c.Cancel());
            base.OnStopIndexing();
        }

        /// <summary>Called when the pause has indexing.</summary>
        protected override void OnPauseIndexing()
        {
            currentCrawlOperations.Values.ToList().ForEach(c => c.Pause());
            base.OnPauseIndexing();
        }

        /// <summary>Called when the resume has indexing.</summary>
        protected override void OnResumeIndexing()
        {
            currentCrawlOperations.Values.ToList().ForEach(c => c.Resume());
            base.OnResumeIndexing();
        }

        /// <summary>Adds the specified context.</summary>
        /// <param name="context">The context.</param>
        protected override void Add(IProviderUpdateContext context)
        {
            Add(context, CancellationToken.None);
        }

        /// <summary>Adds the specified context.</summary>
        /// <param name="context">The context.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        protected override void Add(IProviderUpdateContext context, CancellationToken cancellationToken)
        {
            Assert.ArgumentNotNull(context, "context");

            var state = new CrawlState<CommerceProfileIndexableItem>(context.ParallelOptions, this.index, cancellationToken);

            if (this.GetNumberOfItemsToIndex() > 0)
                CrawlingLog.Log.Info(string.Format("[Index={0}] Crawler: {1} items to index", this.Index.Name, this.GetNumberOfItemsToIndex()));

            try
            {
                currentCrawlOperations[state.Id] = state;

                IEnumerable<CommerceProfileIndexableItem> indexables = this.GetItemsToIndex();

                if (context.IsParallel)
                {
                    try
                    {
                        SecurityState securityState = SecurityDisabler.CurrentValue;
                        SiteContext contextSite = Context.Site;
                       
                        var exceptions = new ConcurrentQueue<Exception>();

                        this.ParallelForeachProxy.ForEach(indexables, state.ParallelOptions, (indexable, loopState) =>
                        {
                            state.WaitUntilUnPaused();

                            if (state.IsCancelled || loopState.ShouldExitCurrentIteration)
                                return;

                            try
                            {
                                if (exceptions.Count == 0)
                                {
                                    bool revertSecurityState = false;
                                    bool revertSiteContext = false;
                                    try
                                    {
                                        if (!(securityState == SecurityDisabler.CurrentValue || (securityState == SecurityState.Enabled && SecurityDisabler.CurrentValue == SecurityState.Default)))
                                        {
                                            SecurityDisabler.Enter(securityState);
                                            revertSecurityState = true;
                                        }
                                        
                                        if (Context.Site != contextSite)
                                        {
                                            SiteContextSwitcher.Enter(contextSite);
                                            revertSiteContext = true;
                                        }
                                        this.DoAdd(context, indexable);
                                    }
                                    finally
                                    {
                                        if (revertSecurityState)
                                        {
                                            SecurityDisabler.Exit();
                                        }

                                        if (revertSiteContext)
                                        {
                                            SiteContextSwitcher.Exit();
                                        }
                                    }
                                    
                                    var currentCrawlCount = Interlocked.Increment(ref state.CrawlCount);
                                    if (currentCrawlCount % LogIndexUpdateCountEveryNTimes == 0)
                                    {
                                        CrawlingLog.Log.Info(string.Format("[Index={0}] Crawler: Added {1} items", this.Index.Name, currentCrawlCount));
                                    }
                                }
                            }
                            catch (Exception ex)
                            {
                                CrawlingLog.Log.Warn(string.Format("[Index={0}] Crawler: Add failed - {1}", this.Index.Name, indexable.UniqueId), ex);

                                if (this.StopOnError)
                                {
                                    exceptions.Enqueue(ex);
                                }
                                else
                                {
                                    Debug.WriteLine(ex);
                                }
                            }
                        });

                        if (this.StopOnError && exceptions.Count > 0)
                        {
                            throw new AggregateException(exceptions);
                        }
                    }
#pragma warning disable 168
                    catch (OperationCanceledException ex)
                    {
                        Debug.WriteLine("Crawl operation cancelled.");
                    }
#pragma warning restore 168
                }
                else
                {
                    foreach (var indexable in indexables)
                    {
                        state.WaitUntilUnPaused();

                        if (state.IsCancelled || state.CancellationToken.IsCancellationRequested)
                        {
                            Debug.WriteLine("Crawl operation cancelled.");
                            break;
                        }

                        try
                        {
                            this.DoAdd(context, indexable);

                            state.CrawlCount++;

                            if (state.CrawlCount % LogIndexUpdateCountEveryNTimes == 0)
                                CrawlingLog.Log.Info(string.Format("[Index={0}] Crawler: Added {1} items", this.Index.Name, state.CrawlCount));
                        }
                        catch (Exception ex)
                        {
                            CrawlingLog.Log.Warn(string.Format("[Index={0}] Crawler: Add failed - {1}", this.Index.Name, indexable.UniqueId), ex);
                            Debug.WriteLine(ex);

                            if (this.StopOnError)
                                throw;
                            else
                                Debug.WriteLine(ex);
                        }
                    }
                }
            }
            finally
            {
                currentCrawlOperations.Remove(state.Id);
            }
        }

        /// <summary>Updates the specified context.</summary>
        /// <param name="context">The context.</param>
        protected override void Update(IProviderUpdateContext context)
        {
            Update(context, CancellationToken.None);
        }

        /// <summary>Updates the specified context.</summary>
        /// <param name="context">The context.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        protected override void Update(IProviderUpdateContext context, CancellationToken cancellationToken)
        {
            Assert.ArgumentNotNull(context, "context");

            var state = new CrawlState<CommerceProfileIndexableItem>(context.ParallelOptions, this.index, cancellationToken);

            if (this.GetNumberOfItemsToIndex() > 0)
                CrawlingLog.Log.Info(string.Format("[Index={0}] Crawler: {1} items to index", this.Index.Name, this.GetNumberOfItemsToIndex()));

            try
            {
                currentCrawlOperations[state.Id] = state;

                var indexables = this.GetItemsToIndex().ToArray();
                if (context.IsParallel && indexables.Any())
                {
                    try
                    {
                        SecurityState securityState = SecurityDisabler.CurrentValue;
                        SiteContext contextSite = Context.Site;

                        var exceptions = new ConcurrentQueue<Exception>();
                        this.ParallelForeachProxy.ForEach(
                          indexables.Where(i => i != null),
                          state.ParallelOptions,
                          (indexable, loopState) =>
                          {
                              state.WaitUntilUnPaused();

                              if (state.IsCancelled || loopState.ShouldExitCurrentIteration)
                              {
                                  CrawlingLog.Log.Debug(string.Format("[Index={0}] Crawler: Canceled - {1}", this.Index.Name, indexable.UniqueId));
                                  return;
                              }

                              try
                              {
                                 if (exceptions.Count == 0)
                                 {
                                     bool revertSecurityState = false;
                                     bool revertSiteContext = false;
                                      try
                                      {
                                          if (!(securityState == SecurityDisabler.CurrentValue || (securityState == SecurityState.Enabled && SecurityDisabler.CurrentValue == SecurityState.Default)))
                                          {
                                              SecurityDisabler.Enter(securityState);
                                              revertSecurityState = true;
                                          }

                                          if (Context.Site != contextSite)
                                          {
                                              SiteContextSwitcher.Enter(contextSite);
                                              revertSiteContext = true;
                                          }
                                          this.DoUpdate(context, indexable);
                                      }
                                      finally
                                      {
                                          if (revertSecurityState)
                                          {
                                              SecurityDisabler.Exit();
                                          }

                                          if (revertSiteContext)
                                          {
                                              SiteContextSwitcher.Exit();
                                          }
                                      }

                                      var currentCrawlCount = Interlocked.Increment(ref state.CrawlCount);
                                      if (currentCrawlCount % LogIndexUpdateCountEveryNTimes == 0)
                                      {
                                          CrawlingLog.Log.Info(string.Format("[Index={0}] Crawler: Updated {1} items", this.Index.Name, currentCrawlCount));
                                      }
                                 }
                              }
                              catch (Exception ex)
                              {
                                  CrawlingLog.Log.Error(string.Format("[Index={0}] Crawler: Update failed - {1}", this.Index.Name, indexable.UniqueId), ex);
                                  if (this.StopOnError)
                                  {
                                      exceptions.Enqueue(ex);
                                  }
                                  else
                                  {
                                      Debug.WriteLine(ex);
                                  }
                              }
                          });

                        CrawlingLog.Log.Info(string.Format("[Index={0}] Crawler: Total updated items {1}", this.Index.Name, state.CrawlCount));

                        if (this.StopOnError && exceptions.Count > 0)
                        {
                            throw new AggregateException(exceptions);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        CrawlingLog.Log.Debug(string.Format("[Index={0}] Crawler: Canceled", this.Index.Name));
                    }
                    catch (Exception ex)
                    {
                        CrawlingLog.Log.Error(string.Format("[Index={0}] Crawler: Error occured", this.Index.Name), ex);
                    }
                }
                else
                {
                    foreach (var indexable in indexables.Where(i => i != null))
                    {
                        state.WaitUntilUnPaused();

                        if (state.IsCancelled || state.CancellationToken.IsCancellationRequested)
                        {
                            Debug.WriteLine("Crawl operation cancelled.");
                            break;
                        }

                        try
                        {
                            this.DoUpdate(context, indexable);

                            state.CrawlCount++;

                            if (state.CrawlCount % LogIndexUpdateCountEveryNTimes == 0)
                                CrawlingLog.Log.Info(string.Format("[Index={0}] Crawler: Updated {1} items", this.Index.Name, state.CrawlCount));
                        }
                        catch (Exception ex)
                        {
                            CrawlingLog.Log.Error(string.Format("[Index={0}] Crawler: Update failed - {1}", this.Index.Name, indexable.UniqueId), ex);
                            if (this.StopOnError)
                            {
                                throw;
                            }

                            Debug.WriteLine(ex);
                        }
                    }
                }
            }
            finally
            {
                currentCrawlOperations.Remove(state.Id);
            }
        }
        private class CrawlState<TCrawl>
        {
            public Guid Id = Guid.NewGuid();
            public TaskScheduler TaskScheduler = TaskScheduler.Default;
            public long CrawlCount = 0;
            public CancellationToken CancellationToken = CancellationToken.None;
            public CancellationTokenSource TokenSource = null;
            public ParallelOptions ParallelOptions;
            public ISearchIndex Index;

            public volatile bool IsPaused;
            public volatile bool IsCancelled;

            /// <summary>
            /// Initializes a new instance of the <see cref="CrawlState{T}" /> class.
            /// </summary>
            /// <param name="parallelOptions">The parallel options.</param>
            /// <param name="index">The index.</param>
            /// <param name="cancellationToken">The cancellation token.</param>
            public CrawlState(ParallelOptions parallelOptions, ISearchIndex index, CancellationToken cancellationToken)
            {
                var localTokenSource = new CancellationTokenSource();
                var localCancellationToken = localTokenSource.Token;

                this.TokenSource = CancellationTokenSource.CreateLinkedTokenSource(localCancellationToken, cancellationToken);
                this.CancellationToken = this.TokenSource.Token;

                var options = new ParallelOptions()
                {
                    MaxDegreeOfParallelism = parallelOptions.MaxDegreeOfParallelism,
                    TaskScheduler = this.TaskScheduler,
                    CancellationToken = this.CancellationToken
                };

                this.ParallelOptions = options;
                this.Index = index;
            }

            /// <summary>Waits the until un paused.</summary>
            public void WaitUntilUnPaused()
            {
#if DEBUG
                bool printPauseMessage = true;
                bool wasPaused = false;
                DateTime waitStart = DateTime.Now;
#endif
                while (this.IsPaused && !this.IsCancelled)
                {
#if DEBUG
                    if (printPauseMessage)
                    {
                        Debug.WriteLine(string.Format("WaitUntilUnPaused {0}:{1}", Thread.CurrentThread.Name, Thread.CurrentThread.ManagedThreadId), typeof(FlatDataCrawler<>).Name);
                        printPauseMessage = false;
                        wasPaused = true;
                    }
#endif

                    Thread.Sleep(1000);
                }

#if DEBUG
                if (wasPaused)
                    Debug.WriteLine(
                        string.Format("WaitUntilUnPaused {0}:{1} - Waited {2} ms.", Thread.CurrentThread.Name, Thread.CurrentThread.ManagedThreadId, (DateTime.Now - waitStart).TotalMilliseconds), typeof(FlatDataCrawler<>).Name);
#endif
            }

            /// <summary>Pauses this instance.</summary>
            public void Pause()
            {
                CrawlingLog.Log.Warn(string.Format("[Index={0}][CrawlOperationId={1}] Crawler: Pause Requested", this.Index.Name, this.Id));
                Debug.WriteLine("Crawling Pause Requested", typeof(FlatDataCrawler<>).Name);

                this.IsPaused = true;
            }

            /// <summary>Resumes this instance.</summary>
            public void Resume()
            {
                CrawlingLog.Log.Warn(string.Format("[Index={0}][CrawlOperationId={1}] Crawler: Resume Requested", this.Index.Name, this.Id));
                Debug.WriteLine("Crawling Resume Requested", typeof(FlatDataCrawler<>).Name);

                this.IsPaused = false;
                this.IsCancelled = false;
            }

            /// <summary>Cancels this instance.</summary>
            public void Cancel()
            {
                CrawlingLog.Log.Warn(string.Format("[Index={0}][CrawlOperationId={1}] Crawler: Cancel Requested", this.Index.Name, this.Id));
                Debug.WriteLine("Crawling Cancel Requested", typeof(FlatDataCrawler<>).Name);

                lock (this)
                {
                    Pause();

                    this.IsCancelled = true;

                    if (this.TokenSource != null)
                        this.TokenSource.Cancel();

                    this.CrawlCount = 0;
                }
            }
        }
    }
}