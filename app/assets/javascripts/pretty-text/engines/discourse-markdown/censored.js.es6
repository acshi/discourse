import { censor } from 'pretty-text/censored-words';

export const setup = helper => helper.addPreProcessor(censor);
