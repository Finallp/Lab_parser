package parser;

import org.apache.http.HttpEntity;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class HtmlParser {
    private CloseableHttpClient client = null;
    private static volatile Map<String, Document> docVec;
    private static final Logger logger = LoggerFactory.getLogger(HtmlParser.class);

    public HtmlParser(Map<String, Document> docVec) {
        HtmlParser.docVec = docVec;

        client = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                .setDefaultCookieStore(new BasicCookieStore()).build();
    }

    public void parsePage(String url) throws IOException {
        logger.info(" Received '" + url + "'" + "  " + Thread.currentThread());
        int code = 0;
        boolean bStop = false;
        Document doc = null;
        int retryCount = 3;
        for (int iTry = 0; iTry < retryCount && !bStop; iTry++) {
            int metadataTimeout = 30 * 1000;
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(metadataTimeout)
                    .setConnectTimeout(metadataTimeout)
                    .setConnectionRequestTimeout(metadataTimeout)
                    .setExpectContinueEnabled(true)
                    .build();
            HttpGet request = new HttpGet(url);
            request.setConfig(requestConfig);
            CloseableHttpResponse response = null;
            try {
                logger.debug(Thread.currentThread() + "start");
                response = client.execute(request);
                logger.debug(Thread.currentThread() + "stop");
                code = response.getStatusLine().getStatusCode();
                if (code == 404) {
                    logger.error("error get url " + url + " code " + code);
                    try {
                        response.close();
                    } catch (IOException e) {
                        logger.error(String.valueOf(e));
                    }
                    logger.warn("error get url " + url + " code " + code);
                    bStop = true;
                } else if (code == 200) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        try {
                            doc = Jsoup.parse(entity.getContent(), "UTF-8", url);
                            docVec.put(url, doc);
                            //logger.indexLog(LOGGER_LEVEL_INFO, docVec.size() + "docs downloaded");
                            try {
                                response.close();
                            } catch (IOException e) {
                                logger.error(String.valueOf(e));
                            }
                            break;
                        } catch (IOException e) {
                            logger.error(String.valueOf(e));
                        }
                    }
                    bStop = true;
                } else {
                    logger.warn("error get url " + url + " code " + code);
                    response.close();
                    response = null;
                    client.close();
                    client = HttpClients.custom()
                            .setDefaultRequestConfig(RequestConfig.custom().setCookieSpec(CookieSpecs.STANDARD).build())
                            .setDefaultCookieStore(new BasicCookieStore()).build();
                    int retryDelay = 5 * 1000;
                    int delay = retryDelay * 1000 * (iTry + 1);
                    try {
                        Thread.sleep(delay);
                        continue;
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            } catch (IOException e) {
                logger.error(String.valueOf(e));
            }
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    logger.error(String.valueOf(e));
                }
            }
        }
    }
}
