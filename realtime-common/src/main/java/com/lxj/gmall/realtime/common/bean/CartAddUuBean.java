package com.lxj.gmall.realtime.common.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
/**
 * @author Laoxingjie
 * @description 加购数据实体类
 * @create 2026/4/11 15:14
 **/

@Data
@AllArgsConstructor
public class CartAddUuBean {
    // 窗口起始时间
    String stt;
    // 窗口闭合时间
    String edt;
    // 当天日期
    String curDate;
    // 加购独立用户数
    Long cartAddUuCt;
}
