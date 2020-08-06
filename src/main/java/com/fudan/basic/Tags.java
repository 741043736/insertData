package com.fudan.basic;

public class Tags {
	public static final String[] product_basic_tags=new String[] {
			"ITEM_NUM_ID",
			"ITEM_NAME",
			"ITEMID",
			"ITEM_GRADE",
			"PTY_NUM_1",
			"PTY_NUM_2",
			"PTY_NUM_3",
			"TENANT_NUM_ID"
			,"PURCHASE_TYPE_NUM_ID",
			"BRAND_ID"
	};
	public static final String[] tml_dtl_tags = new String[] {
			"TML_NUM_ID",
			"ITEM_NUM_ID",
			"TENANT_NUM_ID",
			"DEDUCT_AMOUNT",
			"ITEM_NAME",
			"SALES_TYPE_NUM_ID",
			"F_AMOUNT","QTY",
			"SIM_ITEM_NAME"
	};
	public static final String[] tml_hdr_tags=new String[] {
			"DATA_SIGN",
			"VIP_TYPE",
			"TYPE_NUM_ID",
			"TRAN_TYPE_NUM_ID",
			"PRV_NUM_ID",
			"TML_NUM_ID",
			"TENANT_NUM_ID",
			"SO_FROM_TYPE"
			,"SUB_UNIT_NUM_ID",
			"USR_NUM_ID",
			"VIP_NO",
			"CHANNEL_NUM_ID",
			"ORDER_DATE"
	};
	public static final String[] cash_dtl_tags=new String[] {
			"RESERVED_NO",
			"CASH_NUM_ID",
			"PAY_AMOUNT",
			"PAY_TYPE_ID",
			"CASH_TYPE_NUM_ID",
			"DATA_SIGN"
	};
	public static final String[] pay_type_tags=new String[] {
			"PAY_TYPE_ID",
			"PAY_TYPE_NAME",
			"IS_REAL"
	};
	public static final String[] sub_unit_tags=new String[] {
			"SUB_UNIT_NUM_ID",
			"SUB_UNIT_NAME",
			"SUB_UNIT_TYPE"
	};
}
