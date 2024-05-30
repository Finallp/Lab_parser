package elastic_client;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.TotalHits;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import objects.Article;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;

import java.io.IOException;

public class ElasticClient {
    private final String serverUrl;
    private final String apiKey;
    private static final Object indexCreationLock = new Object();

    public ElasticClient(String serverUrl, String apiKey) {
        this.serverUrl=serverUrl;
        this.apiKey=apiKey;
    }

    public ElasticsearchClient elasticRestClient(String articleIndexName) throws IOException {
        RestClient restClient = RestClient
                .builder(HttpHost.create(serverUrl))
                .setDefaultHeaders(new Header[]{
                        new BasicHeader("Authorization", "ApiKey " + apiKey)
                })
                .build();

        ObjectMapper mapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .build();

        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper(mapper));

        ElasticsearchClient esClient = new ElasticsearchClient(transport);

        createIndexWithDateMappingHeadlines(esClient, articleIndexName);

        return esClient;
    }

    private void createIndexWithDateMappingHeadlines(ElasticsearchClient esClient, String articleIndexName) throws IOException {
        synchronized (indexCreationLock) {
            BooleanResponse indexRes = esClient.indices().exists(ex -> ex.index(articleIndexName));
            if (!indexRes.value()) {
                esClient.indices().create(c -> c
                        .index(articleIndexName)
                        .mappings(m -> m
                                .properties("id", p -> p.keyword(d -> d))
                                .properties("header", p -> p.text(d -> d.fielddata(true)))
                                .properties("body", p -> p.text(d -> d.fielddata(true)))
                                .properties("author", p -> p.text(d -> d.fielddata(true)))
                                .properties("URL", p -> p.keyword(d -> d))
                                .properties("date", p -> p
                                        .date(d -> d.format("strict_date_optional_time")))
                        ));

            }
        }
    }

    public static boolean NewsHeadlineNotExist(ElasticsearchClient elcClient, String index, String id) throws IOException {
        SearchResponse<Article> response = elcClient.search(s -> s
                        .index(index)
                        .query(q -> q
                                .match(t -> t
                                        .field("id")
                                        .query(id)
                                )
                        ),
                Article.class
        );

        TotalHits total = response.hits().total();

        assert total != null;
        return total.value() <= 0;
    }
}
