package parser;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import objects.Article;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import static elastic_client.ElasticClient.NewsHeadlineNotExist;

public class ElcConsumer {
    private final ObjectMapper mapper = new ObjectMapper();
    private final ElasticsearchClient elcClient;
    private final String ARTICLES_INDEX;
    private static final Logger logger = LoggerFactory.getLogger(ElcConsumer.class);

    public ElcConsumer(String serverUrl, String apiKey, String articlesIndex) throws IOException {
        ARTICLES_INDEX = articlesIndex;
        elastic_client.ElasticClient ec = new elastic_client.ElasticClient(serverUrl, apiKey);
        elcClient = ec.elasticRestClient(ARTICLES_INDEX);
        mapper.registerModule(new JodaModule());
    }

    public void consume(String msg) throws IOException {
        logger.debug(Thread.currentThread() + "start");

        try {
            Article nh = new Article();
            JsonNode newsHeadlineJsonNode = mapper.readTree(msg);

            nh.SetAuthor(newsHeadlineJsonNode.get("author").asText());

            nh.SetBody(newsHeadlineJsonNode.get("body").asText());

            nh.SetHeader(newsHeadlineJsonNode.get("header").asText());

            nh.SetDate(newsHeadlineJsonNode.get("date").asText());

            nh.SetURL(newsHeadlineJsonNode.get("URL").asText());

            nh.SetId();

            if (NewsHeadlineNotExist(elcClient, ARTICLES_INDEX, nh.GetId())) {
                IndexRequest<Article> indexReq = IndexRequest.of((id -> id
                        .index(ARTICLES_INDEX)
                        .refresh(Refresh.WaitFor)
                        .document(nh)));

                IndexResponse indexResponse = elcClient.index(indexReq);

                if (indexResponse.result() != null) {
                    logger.info("Document indexed successfully!");
                } else {
                    logger.error("Error occurred during indexing!");
                }
            }
        } catch (IOException e) {
            logger.error(String.valueOf(e));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }

        logger.debug(Thread.currentThread() + "stop");
    }
}
