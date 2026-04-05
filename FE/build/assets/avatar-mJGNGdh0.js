import{c as S,aQ as $,r as l,j as p,m as H,P as L,aH as F,aI as A,w as k}from"./index-DFJIAshN.js";/**
 * @license lucide-react v0.552.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const P=[["line",{x1:"12",x2:"12",y1:"2",y2:"22",key:"7eqyqh"}],["path",{d:"M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6",key:"1b0p4s"}]],ee=S("dollar-sign",P);/**
 * @license lucide-react v0.552.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const T=[["path",{d:"m15.5 7.5 2.3 2.3a1 1 0 0 0 1.4 0l2.1-2.1a1 1 0 0 0 0-1.4L19 4",key:"g0fldk"}],["path",{d:"m21 2-9.6 9.6",key:"1j0ho8"}],["circle",{cx:"7.5",cy:"15.5",r:"5.5",key:"yqb3hr"}]],te=S("key",T);/**
 * @license lucide-react v0.552.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const V=[["path",{d:"M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8",key:"1357e3"}],["path",{d:"M3 3v5h5",key:"1xhq8a"}]],ae=S("rotate-ccw",V);/**
 * @license lucide-react v0.552.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const D=[["path",{d:"M10 11v6",key:"nco0om"}],["path",{d:"M14 11v6",key:"outv1u"}],["path",{d:"M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6",key:"miytrc"}],["path",{d:"M3 6h18",key:"d0wm0j"}],["path",{d:"M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2",key:"e791ji"}]],re=S("trash-2",D);var x={exports:{}},E={};/**
 * @license React
 * use-sync-external-store-shim.production.js
 *
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */var _;function z(){if(_)return E;_=1;var e=$();function t(r,o){return r===o&&(r!==0||1/r===1/o)||r!==r&&o!==o}var s=typeof Object.is=="function"?Object.is:t,n=e.useState,u=e.useEffect,a=e.useLayoutEffect,f=e.useDebugValue;function i(r,o){var m=o(),y=n({inst:{value:m,getSnapshot:o}}),d=y[0].inst,g=y[1];return a(function(){d.value=m,d.getSnapshot=o,c(d)&&g({inst:d})},[r,m,o]),u(function(){return c(d)&&g({inst:d}),r(function(){c(d)&&g({inst:d})})},[r]),f(m),m}function c(r){var o=r.getSnapshot;r=r.value;try{var m=o();return!s(r,m)}catch{return!0}}function v(r,o){return o()}var h=typeof window>"u"||typeof window.document>"u"||typeof window.document.createElement>"u"?v:i;return E.useSyncExternalStore=e.useSyncExternalStore!==void 0?e.useSyncExternalStore:h,E}var R;function K(){return R||(R=1,x.exports=z()),x.exports}var U=K();function G(){return U.useSyncExternalStore(B,()=>!0,()=>!1)}function B(){return()=>{}}var w="Avatar",[O]=H(w),[Q,j]=O(w),I=l.forwardRef((e,t)=>{const{__scopeAvatar:s,...n}=e,[u,a]=l.useState("idle");return p.jsx(Q,{scope:s,imageLoadingStatus:u,onImageLoadingStatusChange:a,children:p.jsx(L.span,{...n,ref:t})})});I.displayName=w;var M="AvatarImage",N=l.forwardRef((e,t)=>{const{__scopeAvatar:s,src:n,onLoadingStatusChange:u=()=>{},...a}=e,f=j(M,s),i=W(n,a),c=F(v=>{u(v),f.onImageLoadingStatusChange(v)});return A(()=>{i!=="idle"&&c(i)},[i,c]),i==="loaded"?p.jsx(L.img,{...a,ref:t,src:n}):null});N.displayName=M;var C="AvatarFallback",q=l.forwardRef((e,t)=>{const{__scopeAvatar:s,delayMs:n,...u}=e,a=j(C,s),[f,i]=l.useState(n===void 0);return l.useEffect(()=>{if(n!==void 0){const c=window.setTimeout(()=>i(!0),n);return()=>window.clearTimeout(c)}},[n]),f&&a.imageLoadingStatus!=="loaded"?p.jsx(L.span,{...u,ref:t}):null});q.displayName=C;function b(e,t){return e?t?(e.src!==t&&(e.src=t),e.complete&&e.naturalWidth>0?"loaded":"loading"):"error":"idle"}function W(e,{referrerPolicy:t,crossOrigin:s}){const n=G(),u=l.useRef(null),a=n?(u.current||(u.current=new window.Image),u.current):null,[f,i]=l.useState(()=>b(a,e));return A(()=>{i(b(a,e))},[a,e]),A(()=>{const c=r=>()=>{i(r)};if(!a)return;const v=c("loaded"),h=c("error");return a.addEventListener("load",v),a.addEventListener("error",h),t&&(a.referrerPolicy=t),typeof s=="string"&&(a.crossOrigin=s),()=>{a.removeEventListener("load",v),a.removeEventListener("error",h)}},[a,s,t]),f}var J=I,X=N,Y=q;function ne({className:e,...t}){return p.jsx(J,{"data-slot":"avatar",className:k("relative flex size-10 shrink-0 overflow-hidden rounded-full",e),...t})}function oe({className:e,...t}){return p.jsx(X,{"data-slot":"avatar-image",className:k("aspect-square size-full",e),...t})}function se({className:e,...t}){return p.jsx(Y,{"data-slot":"avatar-fallback",className:k("bg-muted flex size-full items-center justify-center rounded-full",e),...t})}export{ne as A,ee as D,te as K,ae as R,re as T,oe as a,se as b};
