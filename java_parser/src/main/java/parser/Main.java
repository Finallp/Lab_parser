package parser;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeoutException;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.MgetResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;

import objects.Article;
import scheduler.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    public static final String BasePath = "https://spb.mk.ru";
    public static final int Depth = 1;
    private static final String SERVER_URL = "http://localhost:9200";
    private static final String ARTICLES_INDEX = "articles";
    private static final String API_KEY = "";
    public static ElasticsearchClient elcClient;

    public static void main(String[] args) throws InterruptedException {
        logger.debug("Starting");
        Scheduler threadPool = new Scheduler(7);
        Queue<Runnable> tasks = new ConcurrentLinkedQueue<Runnable>();
        Parser parserEntity = new Parser();

        Runnable runLinkCatcher = () -> {
            try {
                parserEntity.runLinkCatcher(Depth, BasePath);
            } catch (IOException | TimeoutException e) {
                logger.error(String.valueOf(e));
            } catch (NoSuchAlgorithmException e) {
                logger.error(String.valueOf(e));
                throw new RuntimeException(e);
            }
        };

        tasks.add(runLinkCatcher);

        Runnable runHtmlParser = () -> {
            try {
                parserEntity.RunHtmlParserAndElcProducer();
            } catch (IOException | TimeoutException e) {
                logger.error(String.valueOf(e));
            } catch (InterruptedException e) {
                logger.error(String.valueOf(e));
                throw new RuntimeException(e);
            }
        };

        tasks.add(runHtmlParser);
        tasks.add(runHtmlParser);
        tasks.add(runHtmlParser);

        Runnable runElcConsumer = () -> {
            try {
                parserEntity.RunElcConsumer();
            } catch (IOException | TimeoutException e) {
                logger.error(String.valueOf(e));
            } catch (InterruptedException e) {
                logger.error(String.valueOf(e));
                throw new RuntimeException(e);
            }
        };

        tasks.add(runElcConsumer);
        tasks.add(runElcConsumer);
        tasks.add(runElcConsumer);

        threadPool.executeTasks(tasks);

        threadPool.shutdown();

        threadPool.joinAllThreads();

//        ExecuteSearchRequests();
    }

    public static void ExecuteSearchRequests() throws IOException {
        elastic_client.ElasticClient ec = new elastic_client.ElasticClient(SERVER_URL, API_KEY);
        elcClient = ec.elasticRestClient(ARTICLES_INDEX);

        Query byAuthorMatch = MatchQuery.of(m -> m
                .field("author")
                .query("Валерия Федорова")
        )._toQuery();

        Query byBodyGovernorMatch = MatchQuery.of(m -> m
                .field("body")
                .query("губернатор")
        )._toQuery();

        Query byHeaderPeterburgMatch = MatchQuery.of(m -> m
                .field("header")
                .query("Петербург")
        )._toQuery();

        Query byAuthorTermQuery = new Query.Builder().term( t -> t
                .field("author")
                .value(v -> v.stringValue("Валерия Федорова"))
        ).build();

        // AND
        SearchResponse<Article> andResponse = elcClient.search(s -> s
                        .index(ARTICLES_INDEX)
                        .query(q -> q
                                .bool(b -> b
                                        .must(byBodyGovernorMatch, byAuthorMatch)//, byHeaderPeterburgMatch)
                                )
                        ),
                Article.class
        );

        List<Hit<Article>> andHits = andResponse.hits().hits();
        outputHits(andHits);

        // OR
        SearchResponse<Article> orResponse = elcClient.search(s -> s
                        .index(ARTICLES_INDEX)
                        .query(q -> q
                                .bool(b -> b
                                        .should(byBodyGovernorMatch, byAuthorMatch)
                                )
                        ),
                Article.class
        );

        List<Hit<Article>> orHits = orResponse.hits().hits();
        outputHits(orHits);

        // SCRIPT
        SearchResponse<Article> scriptResponse = elcClient.search(s -> s
                        .index(ARTICLES_INDEX)
                        .query(q -> q
                                .scriptScore(ss -> ss
                                        .query(q1 -> q1
                                                .matchAll(ma -> ma))
                                        .script(scr -> scr
                                                .inline(i -> i
                                                        .source("doc['URL'].value.length()"))))),
                Article.class
        );

        List<Hit<Article>> scriptHits = scriptResponse.hits().hits();
        outputHits(scriptHits);


        // MULTIGET
        MgetResponse<Article> mgetResponse = elcClient.mget(mgq -> mgq
                .index(ARTICLES_INDEX)
                        .docs(d -> d
                                .id("arr7Vo8hasuhDFyPWyIm")
                                .id("V1q0WDj6q03dI9uQAZdf")
                                .id("X7r7Vdsda1sdsa_sdsdd")),

                Article.class
        );
        List<Article> mgetHits = new ArrayList<>();
        mgetHits.add(mgetResponse.docs().getFirst().result().source());
        for (Article article : mgetHits) {
            assert article != null;
            logger.debug("Found headline. Author: " + article.GetAuthor() + " Headline: " + article.GetHeader() + " URL: " + article.GetURL());
        }
        System.out.println();



        // Date Histogram Aggregation
        Aggregation agg1 = Aggregation.of(a -> a.dateHistogram(dha -> dha.field("date").calendarInterval(CalendarInterval.valueOf(String.valueOf(CalendarInterval.Day)))));
        SearchResponse<?> dhAggregation = elcClient.search(s -> s
                        .index(ARTICLES_INDEX)
                        .aggregations("articles_per_day", agg1),
                Article.class
        );

        logger.debug(String.valueOf(dhAggregation));
        System.out.println();

        // Date Range Aggregation
        Aggregation agg2 = Aggregation.of(a -> a.dateRange(dha -> dha.field("date")
                .ranges(dr -> dr
                        .from(FieldDateMath.of(fdm -> fdm.expr("2024-01-01")))
                        .to(FieldDateMath.of(fdm -> fdm.expr("2024-02-01"))))));
        SearchResponse<?> drAggregation = elcClient.search(s -> s
                        .index(ARTICLES_INDEX)
                        .aggregations("articles_in_range", agg2),
                Article.class
        );

        logger.debug(String.valueOf(drAggregation));
        System.out.println();

        // Histogram Aggregation
        Aggregation agg3 = Aggregation.of(a -> a.histogram(dha -> dha.script(scr -> scr
                        .inline(i -> i
                                .source("doc['header'].value.length()")
                                .lang("painless"))
                ).interval(10.0)
                ));
        SearchResponse<?> hAggregation = elcClient.search(s -> s
                        .index(ARTICLES_INDEX)
                        .aggregations("header_length_histogram", agg3),
                Article.class
        );

        logger.debug(String.valueOf(hAggregation));
        System.out.println();

        // Terms Aggregation
        Aggregation agg4 = Aggregation.of(a -> a.terms(t -> t
                .field("author")
                )
        );
        SearchResponse<?> tAggregation = elcClient.search(s -> s
                        .index(ARTICLES_INDEX)
                        .aggregations("popular_authors", agg4),
                Article.class
        );

        logger.debug(String.valueOf(tAggregation));
        System.out.println();

        // Filter Aggregation
        Aggregation agg5_1 = Aggregation.of(a -> a
                .avg(avg -> avg
                        .script(scr -> scr
                                .inline(i -> i
                                        .source("doc['body'].value.length()")
                                        .lang("painless"))
                        )
                )
        );
        Aggregation agg5 = Aggregation.of(a -> a
                .filter(q -> q.term(t -> t
                                .field("author")
                                .value("Валерия Федорова")
                        )
                )
                .aggregations("avg_body_length", agg5_1)
        );
        SearchResponse<?> fAggregation = elcClient.search(s -> s
                        .index(ARTICLES_INDEX)
                        .aggregations("filtered_body", agg5),
                Article.class
        );

        logger.debug(String.valueOf(fAggregation));
        System.out.println();

        // Logs Aggregation
        Aggregation agg6 = Aggregation.of(a -> a.terms(t -> t
                        .field("stream.keyword")
                        .size(10)
                )
        );
        SearchResponse<?> lAggregation = elcClient.search(s -> s
                        .index(ARTICLES_INDEX)
                        .aggregations("streams", agg6)
                        .size(0),
                Article.class
        );

        logger.debug(String.valueOf(lAggregation));
        System.out.println();
    }

    private static void outputHits(List<Hit<Article>> hits) {
        if (hits.isEmpty()) {
            logger.debug("Empty response");
        }
        for (Hit<Article> hit: hits) {
            Article article = hit.source();
            assert article != null;
            logger.debug("Found article. Author: " + article.GetAuthor() + " Article: " + article.GetHeader() + " URL: " + article.GetURL() + " _id: " + hit.id() + " score: " + hit.score());
        }
        System.out.println();
    }
}