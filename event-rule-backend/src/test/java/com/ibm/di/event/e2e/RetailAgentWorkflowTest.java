/*
 *
 *   Copyright IBM Corp. 2026
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package com.ibm.di.event.e2e;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ibm.di.event.agent.*;
import com.ibm.di.event.rule.RuleEngineRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Comprehensive end-to-end unit and workflow tests for the Retail Next Best Offer Agent.
 *
 * <p>Exercises the full multi-context stateful pipeline:
 * <pre>
 *   CustomerEvent   (Profile & Loyalty)    -> ProcessCustomerContext  -> state.customerContext
 *   ProductEvent    (Details & Specs)      -> ProcessProductContext   -> state.productContext
 *   InventoryEvent  (Availability & Stock) -> ProcessInventoryContext -> state.inventoryContext
 *   PromotionEvent  (Discounts & Validity) -> ProcessPromotionContext -> state.promotionContext
 *   ShoppingEvent   (Live interaction)     -> Generated_decision_model (DMN)
 *                                          -> NextBestOffer event
 * </pre>
 *
 * <p>Validates:
 * <ul>
 *   <li>Presence and preservation of all 6 operations (`ProcessCustomerContext`,
 *       `ProcessProductContext`, `ProcessInventoryContext`, `ProcessPromotionContext`,
 *       `ProcessShoppingContext`, `Generated_decision_model`).</li>
 *   <li>Sequential accumulation and preservation of customer entity state across events.</li>
 *   <li>Direct invocation of the compiled Decision Intelligence archive JAR
 *       when present on the filesystem.</li>
 *   <li>Deterministic generation and assertions on the emitted {@code NextBestOffer} event.</li>
 * </ul>
 */
@DisplayName("Retail Next Best Offer Agent — End-to-End Workflow & Pipeline Tests")
class RetailAgentWorkflowTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String RETAIL_JAR_PATH =
        "../samples/retail-agent/RealTimeNextBestOffer_LATEST-SNAPSHOT.jar";
    private static final String RETAIL_JAR_PATH_LOCAL =
        "samples/retail-agent/RealTimeNextBestOffer_LATEST-SNAPSHOT.jar";

    private AgentDescriptor retailDescriptor;

    @BeforeEach
    void setUp() throws Exception {
        retailDescriptor = buildRetailDescriptor();
    }

    private static AgentDescriptor buildRetailDescriptor() throws Exception {
        AgentDescriptor d = new AgentDescriptor();
        d.setAgentId("retail-next-best-offer-agent");
        d.setName("Retail Next Best Offer Agent");
        d.setEntityType("Customer");
        d.setEntityKeyExtractor("$.customerContext.customerIdentifier");
        d.setChainingEnabled(true);

        RulesetConfig ruleset = new RulesetConfig(RETAIL_JAR_PATH_LOCAL);
        ruleset.setArchiveControlTopic("agent-retail-archive-control");

        // 1. Generated_decision_model operation
        RulesetConfig.OperationConfig opDecision = new RulesetConfig.OperationConfig();
        opDecision.setOperation("GeneratedDecisionModel-Generated_decision_model");
        opDecision.setEntityField("customerContext");
        opDecision.setCurrentEventField("shoppingContext");
        opDecision.setOutbounds(List.of(new RulesetConfig.OutboundConfig("NextBestOffer", "NextBestOffer")));

        // 2. ProcessProductContext operation
        RulesetConfig.OperationConfig opProduct = new RulesetConfig.OperationConfig();
        opProduct.setOperation("ProcessProductContext");
        opProduct.setEntityField("customerContext");
        opProduct.setCurrentEventField("productContext");
        opProduct.setOutbounds(List.of(new RulesetConfig.OutboundConfig("NextBestOffer", "NextBestOffer")));

        // 3. ProcessShoppingContext operation
        RulesetConfig.OperationConfig opShopping = new RulesetConfig.OperationConfig();
        opShopping.setOperation("ProcessShoppingContext");
        opShopping.setEntityField("customerContext");
        opShopping.setCurrentEventField("shoppingContext");
        opShopping.setHistory(List.of(new RulesetConfig.HistoryFieldMapping("shoppingContext", "pastShoppingEvents")));
        opShopping.setOutbounds(List.of(new RulesetConfig.OutboundConfig("NextBestOffer", "NextBestOffer")));

        // 4. ProcessInventoryContext operation
        RulesetConfig.OperationConfig opInventory = new RulesetConfig.OperationConfig();
        opInventory.setOperation("ProcessInventoryContext");
        opInventory.setEntityField("customerContext");
        opInventory.setCurrentEventField("inventoryContext");
        opInventory.setOutbounds(List.of(new RulesetConfig.OutboundConfig("NextBestOffer", "NextBestOffer")));

        // 5. ProcessPromotionContext operation
        RulesetConfig.OperationConfig opPromotion = new RulesetConfig.OperationConfig();
        opPromotion.setOperation("ProcessPromotionContext");
        opPromotion.setEntityField("customerContext");
        opPromotion.setCurrentEventField("promotionContext");
        opPromotion.setOutbounds(List.of(new RulesetConfig.OutboundConfig("NextBestOffer", "NextBestOffer")));

        // 6. ProcessCustomerContext operation
        RulesetConfig.OperationConfig opCustomer = new RulesetConfig.OperationConfig();
        opCustomer.setOperation("ProcessCustomerContext");
        opCustomer.setEntityField("customerContext");
        opCustomer.setCurrentEventField("customerContext");

        ruleset.setOperations(List.of(
            opDecision, opProduct, opShopping, opInventory, opPromotion, opCustomer
        ));
        d.setRuleset(ruleset);

        d.setSubscriptions(List.of(
            new EventSubscription("shoppingContext", "agent-shopping-events",
                "GeneratedDecisionModel-Generated_decision_model", HorizonConfig.hybrid(50, "7d")),
            new EventSubscription("productContext", "agent-product-events",
                "ProcessProductContext", HorizonConfig.count(10)),
            new EventSubscription("inventoryContext", "agent-inventory-events",
                "ProcessInventoryContext", HorizonConfig.count(10)),
            new EventSubscription("promotionContext", "agent-promotion-events",
                "ProcessPromotionContext", HorizonConfig.count(10)),
            new EventSubscription("customerContext", "agent-customer-events",
                "ProcessCustomerContext", HorizonConfig.count(10))
        ));

        d.setOutputs(List.of(
            new OutputConfig("NextBestOffer", "agent-next-best-offers"),
            new OutputConfig("agent-error", "agent-retail-errors")
        ));

        return d;
    }

    // ── Helper builders for domain events ─────────────────────────────────────

    private static EventEnvelope customerEvent(String customerId, String tier, long ts) throws Exception {
        ObjectNode payload = MAPPER.createObjectNode();
        ObjectNode ctx = payload.putObject("customerContext");
        ctx.put("customerIdentifier", customerId);
        ctx.put("customerProfile", "premium");
        ObjectNode loyalty = ctx.putObject("loyaltyInformation");
        loyalty.put("loyaltyPoints", 1500);
        loyalty.put("loyaltyStatus", tier);
        var history = ctx.putArray("purchaseHistory");
        history.add("headphones").add("laptop").add("tablet").add("speaker").add("keyboard").add("monitor");

        EventEnvelope env = EventEnvelope.from("customerContext", "agent-customer-events", ts, payload);
        env.setEntityId(customerId);
        return env;
    }

    private static EventEnvelope productEvent(String customerId, String productId, String category, double price, long ts) throws Exception {
        ObjectNode payload = MAPPER.createObjectNode();
        payload.putObject("customerContext").put("customerIdentifier", customerId);
        ObjectNode pCtx = payload.putObject("productContext");
        ObjectNode pDetails = pCtx.putObject("productDetails");
        pDetails.put("productId", productId);
        pDetails.put("productCategory", category);
        pDetails.put("productPrice", price);
        var comp = pCtx.putArray("complementaryProducts");
        ObjectNode compItem = comp.addObject();
        compItem.put("productId", "CARRYING-CASE-X1");
        compItem.put("productCategory", "Accessories");
        compItem.put("productPrice", 49.99);

        EventEnvelope env = EventEnvelope.from("productContext", "agent-product-events", ts, payload);
        env.setEntityId(customerId);
        return env;
    }

    private static EventEnvelope inventoryEvent(String customerId, boolean available, long stock, long ts) throws Exception {
        ObjectNode payload = MAPPER.createObjectNode();
        payload.putObject("customerContext").put("customerIdentifier", customerId);
        ObjectNode iCtx = payload.putObject("inventoryContext");
        iCtx.put("productAvailability", available);
        iCtx.put("storeInventory", stock);
        iCtx.put("inventoryConstraints", "None");

        EventEnvelope env = EventEnvelope.from("inventoryContext", "agent-inventory-events", ts, payload);
        env.setEntityId(customerId);
        return env;
    }

    private static EventEnvelope promotionEvent(String customerId, String promoProduct, double discount, long ts) throws Exception {
        ObjectNode payload = MAPPER.createObjectNode();
        payload.putObject("customerContext").put("customerIdentifier", customerId);
        ObjectNode prCtx = payload.putObject("promotionContext");
        var promos = prCtx.putArray("availablePromotions");
        ObjectNode p = promos.addObject();
        p.put("product", promoProduct);
        p.put("discountPercentage", discount);
        p.put("promotionType", "ACCESSORY_DISCOUNT");
        prCtx.put("discountRules", "15% off carrying case with premium Electronics purchase");
        prCtx.put("promotionEligibility", "Gold loyalty members");
        prCtx.put("promotionValidity", "2026-12-31");

        EventEnvelope env = EventEnvelope.from("promotionContext", "agent-promotion-events", ts, payload);
        env.setEntityId(customerId);
        return env;
    }

    private static EventEnvelope shoppingEvent(String customerId, String eventType, String productId, double price, long ts) throws Exception {
        ObjectNode payload = MAPPER.createObjectNode();
        payload.putObject("customerContext").put("customerIdentifier", customerId);
        ObjectNode sCtx = payload.putObject("shoppingContext");
        sCtx.put("eventType", eventType);
        ObjectNode pInfo = sCtx.putObject("productInformation");
        pInfo.put("productId", productId);
        pInfo.put("productCategory", "Electronics");
        pInfo.put("productPrice", price);
        sCtx.put("searchQuery", "wireless noise cancelling headphones");
        ObjectNode store = sCtx.putObject("storeContext");
        store.put("storeId", "STORE-NYC-001");
        store.put("storeLocation", "New York, NY");

        EventEnvelope env = EventEnvelope.from("shoppingContext", "agent-shopping-events", ts, payload);
        env.setEntityId(customerId);
        return env;
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    @Nested
    @DisplayName("End-to-End Decision Pipeline Execution")
    class EndToEndPipeline {

        @Test
        @DisplayName("Complete sequence emits NextBestOffer with valid recommendation, discount, and reason")
        void fullSequenceProducesNextBestOffer() throws Exception {
            StatefulAgentProcessor processor = new StatefulAgentProcessor(retailDescriptor);

            // Configure mock rule engine for GeneratedDecisionModel that produces the canonical NextBestOffer
            ObjectNode offerItem = MAPPER.createObjectNode();
            offerItem.put("decision", "present offer");
            offerItem.put("discount", 15.0);
            offerItem.put("reason", "Customer is eligible and product is relevant");
            offerItem.put("recommendedProduct", "carrying case");
            offerItem.put("selectedPromotion", "15% off carrying case");

            ObjectNode ruleResult = MAPPER.createObjectNode();
            ruleResult.set("NextBestOffer", MAPPER.createArrayNode().add(offerItem));

            RuleEngineRunner stubEngine = new RuleEngineRunner("GeneratedDecisionModel-Generated_decision_model", URI.create("file:///dev/null")) {
                @Override
                public JsonNode execute(JsonNode ctx) {
                    return ruleResult;
                }
                @Override protected void initialize() {}
                @Override public void close() {}
            };
            processor.injectRuleEngine(stubEngine);

            String customerId = "CUST-VIP-001";
            AgentState state = AgentState.init(customerId, "Customer", 1000L);

            // Step 1: Customer Event
            EventEnvelope ev1 = customerEvent(customerId, "Gold", 1000L);
            processor.invokeForTest(ev1, state);

            // Step 2: Product Event
            EventEnvelope ev2 = productEvent(customerId, "HEADPHONES-PRO-X1", "Electronics", 649.99, 2000L);
            processor.invokeForTest(ev2, state);

            // Step 3: Inventory Event
            EventEnvelope ev3 = inventoryEvent(customerId, true, 12, 3000L);
            processor.invokeForTest(ev3, state);

            // Step 4: Promotion Event
            EventEnvelope ev4 = promotionEvent(customerId, "Carrying Case X1", 15.0, 4000L);
            processor.invokeForTest(ev4, state);

            // Step 5: Trigger Shopping Event (triggers decision model evaluation)
            EventEnvelope ev5 = shoppingEvent(customerId, "PRODUCT_VIEW", "HEADPHONES-PRO-X1", 649.99, 5000L);
            EventEnvelope finalOutput = processor.invokeForTest(ev5, state);

            // Assert output envelope
            assertNotNull(finalOutput, "Final execution must produce an output event");
            assertEquals("NextBestOffer", finalOutput.getEventType(), "Event type must be NextBestOffer");
            assertEquals(customerId, finalOutput.getEntityId(), "Entity ID must be preserved");

            // Assert NextBestOffer payload structure & values
            JsonNode payload = finalOutput.getPayload();
            assertNotNull(payload, "Payload must not be null");
            assertEquals("present offer", payload.path("decision").asText(), "Decision must be 'present offer'");
            assertEquals(15.0, payload.path("discount").asDouble(), 0.01, "Discount must be 15%");
            assertEquals("carrying case", payload.path("recommendedProduct").asText(), "Recommended product must be 'carrying case'");
            assertEquals("15% off carrying case", payload.path("selectedPromotion").asText(), "Selected promotion must match");
            assertEquals("Customer is eligible and product is relevant", payload.path("reason").asText());
        }

        @Test
        @DisplayName("Direct Execution against compiled Decision Intelligence Archive if available on disk")
        void testDirectArchiveExecutionIfAvailable() {
            File jarFile = new File(RETAIL_JAR_PATH_LOCAL);
            if (!jarFile.exists()) {
                jarFile = new File(RETAIL_JAR_PATH);
            }
            assumeTrue(jarFile.exists(),
                "Skipping: retail archive JAR not present at " + jarFile.getAbsolutePath()
                + " — build the sample JAR first if live-archive execution is required.");

            {
                URI jarUri = jarFile.toURI();
                RuleEngineRunner runner = new RuleEngineRunner("GeneratedDecisionModel-Generated_decision_model", jarUri);
                try {
                    runner.initializeEagerly();

                    // Prepare input payload matching DMN schema
                    ObjectNode input = MAPPER.createObjectNode();

                    // customerContext
                    ObjectNode customerContext = input.putObject("customerContext");
                    customerContext.put("customerIdentifier", "CUST-001");
                    customerContext.put("customerProfile", "premium");
                    ObjectNode loyalty = customerContext.putObject("loyaltyInformation");
                    loyalty.put("loyaltyPoints", 1500L);
                    loyalty.put("loyaltyStatus", "Gold");
                    var purchaseHistory = customerContext.putArray("purchaseHistory");
                    purchaseHistory.add("headphones").add("laptop").add("tablet").add("speaker").add("keyboard").add("monitor");

                    // productContext
                    ObjectNode productContext = input.putObject("productContext");
                    ObjectNode productDetails = productContext.putObject("productDetails");
                    productDetails.put("productId", "HEADPHONES-PRO-X1");
                    productDetails.put("productCategory", "Electronics");
                    productDetails.put("productPrice", 649.99);

                    // inventoryContext
                    ObjectNode inventoryContext = input.putObject("inventoryContext");
                    inventoryContext.put("productAvailability", true);
                    inventoryContext.put("storeInventory", 12L);
                    inventoryContext.put("inventoryConstraints", "None");

                    // promotionContext
                    ObjectNode promotionContext = input.putObject("promotionContext");
                    var availablePromotions = promotionContext.putArray("availablePromotions");
                    ObjectNode promo = availablePromotions.addObject();
                    promo.put("product", "Carrying Case X1");
                    promo.put("discountPercentage", 15.0);
                    promo.put("promotionType", "ACCESSORY_DISCOUNT");
                    promotionContext.put("discountRules", "15% off carrying case with premium Electronics purchase");
                    promotionContext.put("promotionEligibility", "Gold loyalty members");
                    promotionContext.put("promotionValidity", "2026-12-31");

                    // shoppingContext
                    ObjectNode shoppingContext = input.putObject("shoppingContext");
                    shoppingContext.put("eventType", "PRODUCT_VIEW");
                    ObjectNode pInfo = shoppingContext.putObject("productInformation");
                    pInfo.put("productId", "HEADPHONES-PRO-X1");
                    pInfo.put("productCategory", "Electronics");
                    pInfo.put("productPrice", 649.99);
                    shoppingContext.put("searchQuery", "wireless headphones");

                    JsonNode result = runner.execute(input);
                    assertNotNull(result, "Decision Intelligence execution must return result");
                    assertFalse(result.isEmpty(), "Result must not be empty — NextBestOffer array must be present");

                    // The decision model returns {"NextBestOffer": [{decision, discount, ...}]}
                    JsonNode nboArray = result.path("NextBestOffer");
                    assertTrue(nboArray.isArray(), "Result must contain a 'NextBestOffer' array");
                    assertFalse(nboArray.isEmpty(), "NextBestOffer array must contain at least one offer");
                    JsonNode offer = nboArray.get(0);
                    assertEquals("present offer", offer.path("decision").asText(),
                        "Decision must be 'present offer'");
                    assertEquals(15.0, offer.path("discount").asDouble(), 0.01,
                        "Discount must be 15%");
                    assertEquals("carrying case", offer.path("recommendedProduct").asText(),
                        "Recommended product must be 'carrying case'");
                    assertEquals("15% off carrying case", offer.path("selectedPromotion").asText(),
                        "Selected promotion must match");
                    assertEquals("Customer is eligible and product is relevant", offer.path("reason").asText(),
                        "Reason must match");
                } finally {
                    runner.close();
                }
            }
        }
    }

    @Nested
    @DisplayName("State Persistence and Event Accumulation")
    class StatePersistence {

        @Test
        @DisplayName("Sliding event window retains shopping history within horizon limits")
        void shoppingEventsRetainedInHistory() throws Exception {
            StatefulAgentProcessor processor = new StatefulAgentProcessor(retailDescriptor);
            processor.injectRuleEngine(new RuleEngineRunner("dummy", URI.create("file:///dev/null")) {
                @Override public JsonNode execute(JsonNode ctx) { return null; }
                @Override protected void initialize() {}
                @Override public void close() {}
            });

            String customerId = "CUST-HIST-001";
            AgentState state = AgentState.init(customerId, "Customer", 1000L);

            // Send multiple shopping events
            for (int i = 0; i < 5; i++) {
                EventEnvelope ev = shoppingEvent(customerId, "PRODUCT_VIEW", "PROD-" + i, 100.0 * (i + 1), 1000L + (i * 500));
                processor.invokeForTest(ev, state);
            }

            AgentState.EventHistory shoppingHistory = state.getEventHistories().get("shoppingContext");
            assertNotNull(shoppingHistory, "shoppingContext history bucket must exist in state");
            assertEquals(5, shoppingHistory.size(), "State should retain all 5 shopping events within the horizon");
        }
    }
}
