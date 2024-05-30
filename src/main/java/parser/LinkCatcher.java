package parser;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import com.rabbitmq.client.Channel;
import objects.ArticleLink;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.Deque;

import static elastic_client.ElasticClient.NewsHeadlineNotExist;

public class LinkCatcher {
    private final String ARTICLES_INDEX;
    private final String URL_QUEUE_NAME;
    private final String baseUrl;
    private final int depth_count;
    public Deque<ArticleLink> urlVec;
    public Deque<ArticleLink> resultUrlVec;
    private final Channel rmqChan;
    private final ElasticsearchClient elcClient;
    private static final Logger logger = LoggerFactory.getLogger(LinkCatcher.class);

    public LinkCatcher(int depth, String inputBaseUrl, String serverUrl, String apiKey, String articlesIndex, String urlQueueName, Channel rmqChannel) throws NoSuchAlgorithmException, IOException {
        ARTICLES_INDEX = articlesIndex;
        URL_QUEUE_NAME = urlQueueName;
        urlVec = new ArrayDeque<ArticleLink>();
        resultUrlVec = new ArrayDeque<ArticleLink>();
        baseUrl = inputBaseUrl;
        urlVec.add(new ArticleLink(baseUrl, 0));
        depth_count = depth;
        rmqChan = rmqChannel;
        elastic_client.ElasticClient ec = new elastic_client.ElasticClient(serverUrl, apiKey);
        elcClient = ec.elasticRestClient(ARTICLES_INDEX);
    }

    public void Start() throws IOException, NoSuchAlgorithmException {
        for (int i = 0; i < depth_count; ++i) {
            fork();
            logger.debug(Thread.currentThread() + "start work");
            urlVec.addAll(resultUrlVec);
        }

        logger.debug(Thread.currentThread() + "end work, " + urlVec.size() + "links");
    }

    private void fork() throws IOException, NoSuchAlgorithmException {
        ArticleLink cur;

        while ((cur = urlVec.pollFirst()) != null) {
            parseUrlAndPublishPage(cur);
        }
    }

    private void parseUrlAndPublishPage(ArticleLink url) throws IOException, NoSuchAlgorithmException {
        int level = url.GetLevel() + 1;

        Document doc = Jsoup.connect(url.GetUrl()).get();
        Elements links = doc.select("a[href]");

        String newUrl;
        for (Element link : links) {
            newUrl = link.attr("abs:href");
            if (
                    !newUrl.startsWith(baseUrl + "/politics/2024/") &&
                            !newUrl.startsWith(baseUrl + "/incident/2024/") &&
                            !newUrl.startsWith(baseUrl + "/culture/2024/") &&
                            !newUrl.startsWith(baseUrl + "/social/2024/") &&
                            !newUrl.startsWith(baseUrl + "/economics/2024/") &&
                            !newUrl.startsWith(baseUrl + "/science/2024/") &&
                            !newUrl.startsWith(baseUrl + "/sport/2024/")
            ) {
                continue;
            }

            if (newUrl.endsWith("#")) {
                newUrl = newUrl.substring(0, newUrl.length() - 1);
            }

            ArticleLink l = new ArticleLink(newUrl, level);
            if (NewsHeadlineNotExist(elcClient, ARTICLES_INDEX, l.GetId())) {
                rmqChan.basicPublish("", URL_QUEUE_NAME, null, newUrl.getBytes(StandardCharsets.UTF_8));
                resultUrlVec.add(l);
            }

            if (level <= this.depth_count) {
                urlVec.add(l);
            }
        }
    }
}
