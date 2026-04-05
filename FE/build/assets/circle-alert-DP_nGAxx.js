import{a5 as e,a6 as t,c as a}from"./index-DFJIAshN.js";class i{static async getOrders(r){return e.get(t.ORDERS.LIST,{params:r})}static async getOrderById(r){return e.get(t.ORDERS.DETAIL(r))}static async createOrder(r){return e.post(t.ORDERS.CREATE,r)}static async updateOrderStatus(r,c){return e.patch(t.ORDERS.UPDATE_STATUS(r),c)}static async cancelOrder(r,c){return e.post(t.USER.CANCEL_ORDER(r),{reason:c})}static async returnOrder(r,c){return e.post(t.USER.RETURN_ORDER(r),c)}static async confirmDelivery(r){return e.post(t.USER.CONFIRM_DELIVERY(r))}static async trackOrder(r){return e.get(t.ORDERS.TRACK(r))}}/**
 * @license lucide-react v0.552.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const s=[["circle",{cx:"12",cy:"12",r:"10",key:"1mglay"}],["line",{x1:"12",x2:"12",y1:"8",y2:"12",key:"1pkeuh"}],["line",{x1:"12",x2:"12.01",y1:"16",y2:"16",key:"4dfq90"}]],y=a("circle-alert",s);export{y as C,i as O};
