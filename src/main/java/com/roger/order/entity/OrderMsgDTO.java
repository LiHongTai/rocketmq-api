package com.roger.order.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@ToString
@AllArgsConstructor
public class OrderMsgDTO {
    private long orderId;
    private String msgType;
}
