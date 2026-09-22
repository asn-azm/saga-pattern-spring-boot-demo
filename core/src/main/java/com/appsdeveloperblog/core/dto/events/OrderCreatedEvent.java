package com.appsdeveloperblog.core.dto.events;

import java.util.UUID;

public class OrderCreatedEvent {

    private UUID orderId;
    private UUID customerId;
    private UUID productId;
    private Integer quantity;


    public OrderCreatedEvent() {
    }

    public OrderCreatedEvent(UUID orderId, UUID customerId, UUID productId, Integer quantity) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
    }


    public UUID getOrderId() {
        return orderId;
    }

    public void setOrderId(UUID orderId) {
        this.orderId = orderId;
    }

    public UUID getCustomerId() {
        return customerId;
    }

    public void setCustomerId(UUID customerId) {
        this.customerId = customerId;
    }

    public UUID getProductId() {
        return productId;
    }

    public void setProductId(UUID productId) {
        this.productId = productId;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }
}
