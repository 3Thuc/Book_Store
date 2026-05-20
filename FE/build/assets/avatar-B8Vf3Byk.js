import{c as m,aW as $,r as l,j as p,n as F,P as A,aM as H,aN as E,x as L}from"./index-0bqPaR7T.js";/**
 * @license lucide-react v0.552.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const P=[["path",{d:"M8 2v4",key:"1cmpym"}],["path",{d:"M16 2v4",key:"4m81vk"}],["rect",{width:"18",height:"18",x:"3",y:"4",rx:"2",key:"1hopcy"}],["path",{d:"M3 10h18",key:"8toen8"}]],te=m("calendar",P);/**
 * @license lucide-react v0.552.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const T=[["line",{x1:"12",x2:"12",y1:"2",y2:"22",key:"7eqyqh"}],["path",{d:"M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6",key:"1b0p4s"}]],ae=m("dollar-sign",T);/**
 * @license lucide-react v0.552.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const V=[["path",{d:"m15.5 7.5 2.3 2.3a1 1 0 0 0 1.4 0l2.1-2.1a1 1 0 0 0 0-1.4L19 4",key:"g0fldk"}],["path",{d:"m21 2-9.6 9.6",key:"1j0ho8"}],["circle",{cx:"7.5",cy:"15.5",r:"5.5",key:"yqb3hr"}]],re=m("key",V);/**
 * @license lucide-react v0.552.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const D=[["path",{d:"M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8",key:"1357e3"}],["path",{d:"M3 3v5h5",key:"1xhq8a"}]],ne=m("rotate-ccw",D);/**
 * @license lucide-react v0.552.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const z=[["path",{d:"M10 11v6",key:"nco0om"}],["path",{d:"M14 11v6",key:"outv1u"}],["path",{d:"M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6",key:"miytrc"}],["path",{d:"M3 6h18",key:"d0wm0j"}],["path",{d:"M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2",key:"e791ji"}]],oe=m("trash-2",z);var x={exports:{}},k={};/**
 * @license React
 * use-sync-external-store-shim.production.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var w;function K(){if(w)return k;w=1;var e=$();function t(r,o){return r===o&&(r!==0||1/r===1/o)||r!==r&&o!==o}var s=typeof Object.is=="function"?Object.is:t,n=e.useState,u=e.useEffect,a=e.useLayoutEffect,f=e.useDebugValue;function c(r,o){var h=o(),S=n({inst:{value:h,getSnapshot:o}}),d=S[0].inst,g=S[1];return a(function(){d.value=h,d.getSnapshot=o,i(d)&&g({inst:d})},[r,h,o]),u(function(){return i(d)&&g({inst:d}),r(function(){i(d)&&g({inst:d})})},[r]),f(h),h}function i(r){var o=r.getSnapshot;r=r.value;try{var h=o();return!s(r,h)}catch{return!0}}function v(r,o){return o()}var y=typeof window>"u"||typeof window.document>"u"||typeof window.document.createElement>"u"?v:c;return k.useSyncExternalStore=e.useSyncExternalStore!==void 0?e.useSyncExternalStore:y,k}var M;function U(){return M||(M=1,x.exports=K()),x.exports}var G=U();function W(){return G.useSyncExternalStore(B,()=>!0,()=>!1)}function B(){return()=>{}}var _="Avatar",[O]=F(_),[J,b]=O(_),j=l.forwardRef((e,t)=>{const{__scopeAvatar:s,...n}=e,[u,a]=l.useState("idle");return p.jsx(J,{scope:s,imageLoadingStatus:u,onImageLoadingStatusChange:a,children:p.jsx(A.span,{...n,ref:t})})});j.displayName=_;var I="AvatarImage",N=l.forwardRef((e,t)=>{const{__scopeAvatar:s,src:n,onLoadingStatusChange:u=()=>{},...a}=e,f=b(I,s),c=Q(n,a),i=H(v=>{u(v),f.onImageLoadingStatusChange(v)});return E(()=>{c!=="idle"&&i(c)},[c,i]),c==="loaded"?p.jsx(A.img,{...a,ref:t,src:n}):null});N.displayName=I;var C="AvatarFallback",q=l.forwardRef((e,t)=>{const{__scopeAvatar:s,delayMs:n,...u}=e,a=b(C,s),[f,c]=l.useState(n===void 0);return l.useEffect(()=>{if(n!==void 0){const i=window.setTimeout(()=>c(!0),n);return()=>window.clearTimeout(i)}},[n]),f&&a.imageLoadingStatus!=="loaded"?p.jsx(A.span,{...u,ref:t}):null});q.displayName=C;function R(e,t){return e?t?(e.src!==t&&(e.src=t),e.complete&&e.naturalWidth>0?"loaded":"loading"):"error":"idle"}function Q(e,{referrerPolicy:t,crossOrigin:s}){const n=W(),u=l.useRef(null),a=n?(u.current||(u.current=new window.Image),u.current):null,[f,c]=l.useState(()=>R(a,e));return E(()=>{c(R(a,e))},[a,e]),E(()=>{const i=r=>()=>{c(r)};if(!a)return;const v=i("loaded"),y=i("error");return a.addEventListener("load",v),a.addEventListener("error",y),t&&(a.referrerPolicy=t),typeof s=="string"&&(a.crossOrigin=s),()=>{a.removeEventListener("load",v),a.removeEventListener("error",y)}},[a,s,t]),f}var X=j,Y=N,Z=q;function se({className:e,...t}){return p.jsx(X,{"data-slot":"avatar",className:L("relative flex size-10 shrink-0 overflow-hidden rounded-full",e),...t})}function ue({className:e,...t}){return p.jsx(Y,{"data-slot":"avatar-image",className:L("aspect-square size-full",e),...t})}function ce({className:e,...t}){return p.jsx(Z,{"data-slot":"avatar-fallback",className:L("bg-muted flex size-full items-center justify-center rounded-full",e),...t})}export{se as A,te as C,ae as D,re as K,ne as R,oe as T,ue as a,ce as b};
